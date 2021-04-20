// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Date 2014/09/24 16:01:08

#ifndef  BVAR_REDUCER_H
#define  BVAR_REDUCER_H

#include <limits>                                 // std::numeric_limits
#include "butil/logging.h"                         // LOG()
#include "butil/type_traits.h"                     // butil::add_cr_non_integral
#include "butil/class_name.h"                      // class_name_str
#include "bvar/variable.h"                        // Variable
#include "bvar/detail/combiner.h"                 // detail::AgentCombiner
#include "bvar/detail/sampler.h"                  // ReducerSampler
#include "bvar/detail/series.h"
#include "bvar/window.h"

namespace bvar {

// Reduce multiple values into one with `Op': e1 Op e2 Op e3 ...
// `Op' shall satisfy:
//   - associative:     a Op (b Op c) == (a Op b) Op c
//   - commutative:     a Op b == b Op a;
//   - no side effects: a Op b never changes if a and b are fixed. 
// otherwise the result is undefined.
//
// For performance issues, we don't let Op return value, instead it shall
// set the result to the first parameter in-place. Namely to add two values,
// "+=" should be implemented rather than "+".
//
// Reducer works for non-primitive T which satisfies:
//   - T() should be the identity of Op.
//   - stream << v should compile and put description of v into the stream
// Example:
// class MyType {
// friend std::ostream& operator<<(std::ostream& os, const MyType&);
// public:
//     MyType() : _x(0) {}
//     explicit MyType(int x) : _x(x) {}
//     void operator+=(const MyType& rhs) const {
//         _x += rhs._x;
//     }
// private:
//     int _x;
// };
// std::ostream& operator<<(std::ostream& os, const MyType& value) {
//     return os << "MyType{" << value._x << "}";
// }
// bvar::Adder<MyType> my_type_sum;
// my_type_sum << MyType(1) << MyType(2) << MyType(3);
// LOG(INFO) << my_type_sum;  // "MyType{6}"
/**
 * Variable中主要是曝光相关的
 * Reducer是一个三个模板参数的模板类，分别是数据类型 T ；reduce操作符 Op ；Op的逆向操作符 InvOp ，默认是VoidOp，也就是没有逆向操作符, InvOp 在定时采样的时候会用到。
 */

template <typename T, typename Op, typename InvOp = detail::VoidOp>
class Reducer : public Variable {
public:
    typedef typename detail::AgentCombiner<T, T, Op> combiner_type;
    typedef typename combiner_type::Agent agent_type;
    typedef detail::ReducerSampler<Reducer, T, Op, InvOp> sampler_type;
    class SeriesSampler : public detail::Sampler {
    public:
        SeriesSampler(Reducer* owner, const Op& op)
            : _owner(owner), _series(op) {}
        ~SeriesSampler() {}
        void take_sample() override { _series.append(_owner->get_value()); }
        void describe(std::ostream& os) { _series.describe(os, NULL); }
    private:
        Reducer* _owner;
        detail::Series<T, Op> _series;
    };

public:
    // The `identify' must satisfy: identity Op a == a
    Reducer(typename butil::add_cr_non_integral<T>::type identity = T(),
            const Op& op = Op(),
            const InvOp& inv_op = InvOp())
        : _combiner(identity, identity, op)
        , _sampler(NULL)         // 只有被window之类的追踪了才会去新建 sampler ，单纯的使用reducer不需要 sampler
        , _series_sampler(NULL)
        , _inv_op(inv_op) {
    }

    ~Reducer() {
        // Calling hide() manually is a MUST required by Variable.
        hide();
        if (_sampler) {
            _sampler->destroy();
            _sampler = NULL;
        }
        if (_series_sampler) {
            _series_sampler->destroy();
            _series_sampler = NULL;
        }
    }

    // Add a value.
    // Returns self reference for chaining.
    Reducer& operator<<(typename butil::add_cr_non_integral<T>::type value);

    // Get reduced value.
    // Notice that this function walks through threads that ever add values
    // into this reducer. You should avoid calling it frequently.
    // 
    // 通过combiner聚合各个agent的值，不过要注意上面的check，这个check是说如果当前bvar已被window跟踪定时采样而且不存在invop，
    // 比如Maxer，是不能调用这个函数的，因为依赖tls的关系，对于这类没有invop的reducer，在定时采样（take_sample）的时候需要reset agent，
    // 调用这个函数取不到正确值。
    T get_value() const {
        CHECK(!(butil::is_same<InvOp, detail::VoidOp>::value) || _sampler == NULL)
            << "You should not call Reducer<" << butil::class_name_str<T>()
            << ", " << butil::class_name_str<Op>() << ">::get_value() when a"
            << " Window<> is used because the operator does not have inverse.";
        return _combiner.combine_agents();
    }


    // Reset the reduced value to T().
    // Returns the reduced value before reset.
    T reset() { return _combiner.reset_all_agents(); }

    void describe(std::ostream& os, bool quote_string) const override {
        if (butil::is_same<T, std::string>::value && quote_string) {
            os << '"' << get_value() << '"';
        } else {
            os << get_value();
        }
    }
    
#ifdef BAIDU_INTERNAL
    void get_value(boost::any* value) const override { *value = get_value(); }
#endif

    // True if this reducer is constructed successfully.
    bool valid() const { return _combiner.valid(); }

    // Get instance of Op.
    const Op& op() const { return _combiner.op(); }
    const InvOp& inv_op() const { return _inv_op; }
    
    sampler_type* get_sampler() {
        if (NULL == _sampler) {
            _sampler = new sampler_type(this);
            _sampler->schedule();
        }
        return _sampler;
    }

    int describe_series(std::ostream& os, const SeriesOptions& options) const override {
        if (_series_sampler == NULL) {
            return 1;
        }
        if (!options.test_only) {
            _series_sampler->describe(os);
        }
        return 0;
    }
    
protected:
    int expose_impl(const butil::StringPiece& prefix,
                    const butil::StringPiece& name,
                    DisplayFilter display_filter) override {
        const int rc = Variable::expose_impl(prefix, name, display_filter);
        if (rc == 0 &&
            _series_sampler == NULL &&
            !butil::is_same<InvOp, detail::VoidOp>::value &&
            !butil::is_same<T, std::string>::value &&
            FLAGS_save_series) {
            _series_sampler = new SeriesSampler(this, _combiner.op());
            _series_sampler->schedule();
        }
        return rc;
    }

private:
    /*
    typedef typename detail::AgentCombiner<T, T, Op> combiner_type;
    typedef typename combiner_type::Agent agent_type;
    typedef detail::ReducerSampler<Reducer, T, Op, InvOp> sampler_type;
     */
    combiner_type   _combiner;
    sampler_type* _sampler;             // 两种sampler因为不是必须的，所以是指针
    SeriesSampler* _series_sampler;
    InvOp _inv_op;
};

template <typename T, typename Op, typename InvOp>
inline Reducer<T, Op, InvOp>& Reducer<T, Op, InvOp>::operator<<(
    typename butil::add_cr_non_integral<T>::type value) {
    // It's wait-free for most time
    // 首先是调用combiner的get_or_create_tls_agent函数拿到当前线程的agent，内部实现上就是tls的相关操作，
    // 如果还未分配则会新建agent，得到agent之后用操作符和值进行修改操作
    agent_type* agent = _combiner.get_or_create_tls_agent();
    if (__builtin_expect(!agent, 0)) {
        LOG(FATAL) << "Fail to create agent";
        return *this;
    }
    agent->element.modify(_combiner.op(), value);
    return *this;
}

// =================== Common reducers ===================

// bvar::Adder<int> sum;
// sum << 1 << 2 << 3 << 4;
// LOG(INFO) << sum.get_value(); // 10
// Commonly used functors
namespace detail {
template <typename Tp>
struct AddTo {
    void operator()(Tp & lhs, 
                    typename butil::add_cr_non_integral<Tp>::type rhs) const
    { lhs += rhs; }
};
template <typename Tp>
struct MinusFrom {
    void operator()(Tp & lhs, 
                    typename butil::add_cr_non_integral<Tp>::type rhs) const
    { lhs -= rhs; }
};
}
template <typename T>
class Adder : public Reducer<T, detail::AddTo<T>, detail::MinusFrom<T> > {
public:
    // AddTo -> OP,  MinusFrom -> Invop
    typedef Reducer<T, detail::AddTo<T>, detail::MinusFrom<T> > Base;
    typedef T value_type;
    typedef typename Base::sampler_type sampler_type;
public:
    // 三个构造函数分别对应不全局曝光、不指定前缀曝光指定和前缀曝光的场景
    Adder() : Base() {}
    explicit Adder(const butil::StringPiece& name) : Base() {
        this->expose(name);
    }
    Adder(const butil::StringPiece& prefix,
          const butil::StringPiece& name) : Base() {
        this->expose_as(prefix, name);
    }
    ~Adder() { Variable::hide(); }
};

// bvar::Maxer<int> max_value;
// max_value << 1 << 2 << 3 << 4;
// LOG(INFO) << max_value.get_value(); // 4
namespace detail {
template <typename Tp> 
struct MaxTo {
    void operator()(Tp & lhs, 
                    typename butil::add_cr_non_integral<Tp>::type rhs) const {
        // Use operator< as well.
        if (lhs < rhs) {
            lhs = rhs;
        }
    }
};
class LatencyRecorderBase;
}
template <typename T>
class Maxer : public Reducer<T, detail::MaxTo<T> > {
public:
    typedef Reducer<T, detail::MaxTo<T> > Base;
    typedef T value_type;
    typedef typename Base::sampler_type sampler_type;
public:
    Maxer() : Base(std::numeric_limits<T>::min()) {}
    explicit Maxer(const butil::StringPiece& name)
        : Base(std::numeric_limits<T>::min()) {
        this->expose(name);
    }
    Maxer(const butil::StringPiece& prefix, const butil::StringPiece& name)
        : Base(std::numeric_limits<T>::min()) {
        this->expose_as(prefix, name);
    }
    ~Maxer() { Variable::hide(); }
private:
    friend class detail::LatencyRecorderBase;
    // The following private funcition a now used in LatencyRecorder,
    // it's dangerous so we don't make them public
    explicit Maxer(T default_value) : Base(default_value) {
    }
    Maxer(T default_value, const butil::StringPiece& prefix,
          const butil::StringPiece& name) 
        : Base(default_value) {
        this->expose_as(prefix, name);
    }
    Maxer(T default_value, const butil::StringPiece& name) : Base(default_value) {
        this->expose(name);
    }
};

// bvar::Miner<int> min_value;
// min_value << 1 << 2 << 3 << 4;
// LOG(INFO) << min_value.get_value(); // 1
namespace detail {

template <typename Tp> 
struct MinTo {
    void operator()(Tp & lhs, 
                    typename butil::add_cr_non_integral<Tp>::type rhs) const {
        if (rhs < lhs) {
            lhs = rhs;
        }
    }
};

}  // namespace detail

template <typename T>
class Miner : public Reducer<T, detail::MinTo<T> > {
public:
    typedef Reducer<T, detail::MinTo<T> > Base;
    typedef T value_type;
    typedef typename Base::sampler_type sampler_type;
public:
    Miner() : Base(std::numeric_limits<T>::max()) {}
    explicit Miner(const butil::StringPiece& name)
        : Base(std::numeric_limits<T>::max()) {
        this->expose(name);
    }
    Miner(const butil::StringPiece& prefix, const butil::StringPiece& name)
        : Base(std::numeric_limits<T>::max()) {
        this->expose_as(prefix, name);
    }
    ~Miner() { Variable::hide(); }
};

}  // namespace bvar

#endif  //BVAR_REDUCER_H
