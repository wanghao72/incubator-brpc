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

// Date: Tue Jul 28 18:15:57 CST 2015

#ifndef  BVAR_DETAIL_SAMPLER_H
#define  BVAR_DETAIL_SAMPLER_H

#include <vector>
#include "butil/containers/linked_list.h"// LinkNode
#include "butil/scoped_lock.h"           // BAIDU_SCOPED_LOCK
#include "butil/logging.h"               // LOG()
#include "butil/containers/bounded_queue.h"// BoundedQueue
#include "butil/type_traits.h"           // is_same
#include "butil/time.h"                  // gettimeofday_us
#include "butil/class_name.h"

namespace bvar {
namespace detail {

// 采集到的采样值使用一个简单的结构体Sample保存，包含值和时间
template <typename T>
struct Sample {
    T data;
    int64_t time_us;

    Sample() : data(), time_us(0) {}
    Sample(const T& data2, int64_t time2) : data(data2), time_us(time2) {}  
};

// The base class for all samplers whose take_sample() are called periodically.
// 全局上sampler都是串起来由一个线程统一调度的
class Sampler : public butil::LinkNode<Sampler> {
public:
    Sampler();
        
    // This function will be called every second(approximately) in a
    // dedicated thread if schedule() is called.
    // 
    // take_sample()：虚函数，需要继承类实现，真正的采样逻辑就在里面
    virtual void take_sample() = 0;

    // Register this sampler globally so that take_sample() will be called
    // periodically.
    // 把当前sampler全局注册以实现周期性地调用take_sample()
    void schedule();

    // Call this function instead of delete to destroy the sampler. Deletion
    // of the sampler may be delayed for seconds.
    // 去除这个sampler，停止采样，实质是将_used置为false，这样collector在执行的时候会将其移出链表
    void destroy();

    /*
    void Sampler::schedule() {
        *butil::get_leaky_singleton<SamplerCollector>() << this;
    }

    void Sampler::destroy() {
        _mutex.lock();
        _used = false;
        _mutex.unlock();
    }
     */
        
protected:
    virtual ~Sampler();
    
friend class SamplerCollector;
    bool _used;
    // Sync destroy() and take_sample().
    butil::Mutex _mutex;
};

// Representing a non-existing operator so that we can test
// is_same<Op, VoidOp>::value to write code for different branches.
// The false branch should be removed by compiler at compile-time.
struct VoidOp {
    template <typename T>
    T operator()(const T&, const T&) const {
        CHECK(false) << "This function should never be called, abort";
        abort();
    }
};

// The sampler for reducer-alike variables.
// The R should have following methods:
//  - T reset();
//  - T get_value();
//  - Op op();
//  - InvOp inv_op();
//  因为只有reducer有window类的需求，继承自Sampler的目前只有ReducerSampler
template <typename R, typename T, typename Op, typename InvOp>
class ReducerSampler : public Sampler {
public:
    static const time_t MAX_SECONDS_LIMIT = 3600;

    // 构造函数的入参是 reducer 的指针，赋值给 _reducer, _window_size 初始值为1，该值可以通过函数改变，构造函数会调用一次采样函数。
    explicit ReducerSampler(R* reducer)
        : _reducer(reducer)
        , _window_size(1) {
        
        // Invoked take_sample at begining so the value of the first second
        // would not be ignored
        take_sample();
    }
    ~ReducerSampler() {}

    // 采样直接执行的采样函数，reduce sampler本身会在构造的时候调用一次，其余都是sampler collector去定期调用。
        void take_sample() override {
        // Make _q ready.
        // If _window_size is larger than what _q can hold, e.g. a larger
        // Window<> is created after running of sampler, make _q larger.
        // 
        // 该函数首先会判断队列容量是否小于_window_size，如果是则扩充队列大小，
        // 实现上是新建一个 BoundedQueue 将item挪过去然后交换，老的队列会在析构的时候释放空间

        if ((size_t)_window_size + 1 > _q.capacity()) {
            const size_t new_cap =
                std::max(_q.capacity() * 2, (size_t)_window_size + 1);
            const size_t memsize = sizeof(Sample<T>) * new_cap;
            void* mem = malloc(memsize);
            if (NULL == mem) {
                return;
            }
            butil::BoundedQueue<Sample<T> > new_q(
                mem, memsize, butil::OWNS_STORAGE);
            Sample<T> tmp;
            while (_q.pop(&tmp)) {
                new_q.push(tmp);
            }
            new_q.swap(_q);
        }

        Sample<T> latest;
        // 根据操作符是否可以逆向来确定调用的reducer函数
        // butil::is_same<InvOp, VoidOp>::value如果为true表明没有InvOp，也就是不能逆向，
        // 比如maxer miner这种reducer，此类reducer只能每次保存完值重置，取值的时候也需要汇总时段内的所有sampler才能得到值，
        // 而有inverse op的，以adder为例，每次记录当前值就行，不需要重置，取值的时候只需首尾要减一下即可。
        if (butil::is_same<InvOp, VoidOp>::value) {
            // The operator can't be inversed.
            // We reset the reducer and save the result as a sample.
            // Suming up samples gives the result within a window.
            // In this case, get_value() of _reducer gives wrong answer and
            // should not be called.
            latest.data = _reducer->reset();
        } else {
            // The operator can be inversed.
            // We save the result as a sample.
            // Inversed operation between latest and oldest sample within a
            // window gives result.
            // get_value() of _reducer can still be called.
            latest.data = _reducer->get_value();
        }
        latest.time_us = butil::gettimeofday_us();
        // 最后通过elim_push将sampler存入队列，elim_push在push前会检测是否full，如果是会pop掉队首。
        _q.elim_push(latest);
    }

    // 用于获取某个时间点到现在的sample值
    // 主要逻辑也是根据是否能够逆向操作采用不同的方式得到时间点对应的值
    // 比如，调用get_value(10, result)，对于 maxer ，那么就是10秒内的最大值，需要使用10秒内的所有采样汇总得到，
    // 对于 adder ，是10秒内的累加值，只需要将最近的值减去十秒前的值即可。
    bool get_value(time_t window_size, Sample<T>* result) {
        if (window_size <= 0) {
            LOG(FATAL) << "Invalid window_size=" << window_size;
            return false;
        }
        BAIDU_SCOPED_LOCK(_mutex);
        if (_q.size() <= 1UL) {
            // We need more samples to get reasonable result.
            return false;
        }
        Sample<T>* oldest = _q.bottom(window_size);
        if (NULL == oldest) {
            oldest = _q.top();
        }
        Sample<T>* latest = _q.bottom();
        DCHECK(latest != oldest);
        if (butil::is_same<InvOp, VoidOp>::value) {
            // No inverse op. Sum up all samples within the window.
            result->data = latest->data;
            for (int i = 1; true; ++i) {
                Sample<T>* e = _q.bottom(i);
                if (e == oldest) {
                    break;
                }
                _reducer->op()(result->data, e->data);
            }
        } else {
            // Diff the latest and oldest sample within the window.
            result->data = latest->data;
            _reducer->inv_op()(result->data, oldest->data);
        }
        result->time_us = latest->time_us - oldest->time_us;
        return true;
    }

    // Change the time window which can only go larger.
    int set_window_size(time_t window_size) {
        if (window_size <= 0 || window_size > MAX_SECONDS_LIMIT) {
            LOG(ERROR) << "Invalid window_size=" << window_size;
            return -1;
        }
        BAIDU_SCOPED_LOCK(_mutex);
        if (window_size > _window_size) {
            _window_size = window_size;
        }
        return 0;
    }

    void get_samples(std::vector<T> *samples, time_t window_size) {
        if (window_size <= 0) {
            LOG(FATAL) << "Invalid window_size=" << window_size;
            return;
        }
        BAIDU_SCOPED_LOCK(_mutex);
        if (_q.size() <= 1) {
            // We need more samples to get reasonable result.
            return;
        }
        Sample<T>* oldest = _q.bottom(window_size);
        if (NULL == oldest) {
            oldest = _q.top();
        }
        for (int i = 1; true; ++i) {
            Sample<T>* e = _q.bottom(i);
            if (e == oldest) {
                break;
            }
            samples->push_back(e->data);
        }
    }

private:
    R* _reducer;                        // sampler对应reducer的指针
    time_t _window_size;                // 当前Sampler的窗口指导大小，称他为指导大小是因为实际大小是由 _q 的大小决定的，在采样过程中会根据_window_size来调整_q的大小
    butil::BoundedQueue<Sample<T> > _q; // 保存采样结果的队列
};

}  // namespace detail
}  // namespace bvar

#endif  // BVAR_DETAIL_SAMPLER_H
