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

// Date 2014/09/22 11:57:43

#ifndef  BVAR_COMBINER_H
#define  BVAR_COMBINER_H

#include <string>                       // std::string
#include <vector>                       // std::vector
#include "butil/atomicops.h"             // butil::atomic
#include "butil/scoped_lock.h"           // BAIDU_SCOPED_LOCK
#include "butil/type_traits.h"           // butil::add_cr_non_integral
#include "butil/synchronization/lock.h"  // butil::Lock
#include "butil/containers/linked_list.h"// LinkNode
#include "bvar/detail/agent_group.h"    // detail::AgentGroup
#include "bvar/detail/is_atomical.h"
#include "bvar/detail/call_op_returning_void.h"

namespace bvar {
namespace detail {

// Parameter to merge_global.
// GlobalValue，全局变量辅助类，保存了 agent 和 combiner 的指针，主要是提供了一些同步机制，用于把 agent 的值往 combiner 的全局结果里合并。
template <typename Combiner>
class GlobalValue {
public:
    typedef typename Combiner::result_type result_type;
    typedef typename Combiner::Agent agent_type;

    GlobalValue(agent_type* a, Combiner* c) : _a(a), _c(c) {}
    ~GlobalValue() {}

    // Call this method to unlock tls element and lock the combiner.
    // Unlocking tls element avoids potential deadlock with
    // AgentCombiner::reset(), which also means that tls element may be
    // changed during calling of this method. BE AWARE OF THIS!
    // After this method is called (and before unlock), tls element and
    // global_result will not be changed provided this method is called
    // from the thread owning the agent.
    result_type* lock() {
        _a->element._lock.Release();
        _c->_lock.Acquire();
        return &_c->_global_result;
    }

    // Call this method to unlock the combiner and lock tls element again.
    void unlock() {
        _c->_lock.Release();
        _a->element._lock.Acquire();
    }

private:
    agent_type* _a;
    Combiner* _c;
};

// Abstraction of tls element whose operations are all atomic.
// ElementContainer是实际元素的容器，也就是对实际类型的封装，之所以需要这么一个封装是因为
// 需要保证多线程操作情况下的同步正确，bvar在本地的 tls 存储除了会被当前线程使用，还会在聚合读取等场景下被其他线程使用
template <typename T, typename Enabler = void>
class ElementContainer {
template <typename> friend class GlobalValue;
public:
    void load(T* out) {  // 读
        butil::AutoLock guard(_lock);
        *out = _value;
    }

    void store(const T& new_value) {  // 写
        butil::AutoLock guard(_lock);
        _value = new_value;
    }

    void exchange(T* prev, const T& new_value) {  // 交换
        butil::AutoLock guard(_lock);
        *prev = _value;
        _value = new_value;
    }

    template <typename Op, typename T1>
    void modify(const Op &op, const T1 &value2) {  // 编辑（根据传入的op和value修改本地value）
        butil::AutoLock guard(_lock);
        call_op_returning_void(op, _value, value2);
    }

    // [Unique]
    template <typename Op, typename GlobalValue>
    void merge_global(const Op &op, GlobalValue & global_value) {
        _lock.Acquire();
        op(global_value, _value);
        _lock.Release();
    }

private:
    T _value;
    butil::Lock _lock;
};

// 除了上边那个通用版本，还有一个针对原子类型变量的特化版本，加锁的开销相对是比较大的，对于int float等本身就带原子性的类型，通过原子操作就能保证同步
template <typename T>
class ElementContainer<
    T, typename butil::enable_if<is_atomical<T>::value>::type> {
    /**
     *   这里将第二个模板参数进行了特化，使用了typename butil::enable_if<is_atomical::value>::type 作为参数，
     * 这里用到了enable_if和SFINAE ，简单来说就是如果条件满足，这个类型就是定义的，否则就是未定义的，
     * 具体到这里，就是如果T是原子类型，那么第二个参数是有定义的，从而能够匹配上这个特化模板，否则只能匹配前面那个更通用的模板。C++里模板的匹配规则是最特化匹配，会优先选择特化程度最高的。
     *   这个特化模板里都是通过原子操作来实现各种操作的，由于不需要额外的数据同步，因此所有的memory_order都是relaxed。
     *   这两个模板都实现了 load store exchange 和 modify ,通用模板里有一个独有函数 merge
     * 而原子变量模板有一个 compare_exchange_weak 是独有的，这和实际用途有关系。


     */
    
     
public:
    // We don't need any memory fencing here, every op is relaxed.
    
    inline void load(T* out) {
        *out = _value.load(butil::memory_order_relaxed);
    }

    inline void store(T new_value) {
        _value.store(new_value, butil::memory_order_relaxed);
    }

    inline void exchange(T* prev, T new_value) {
        *prev = _value.exchange(new_value, butil::memory_order_relaxed);
    }

    // [Unique]
    inline bool compare_exchange_weak(T& expected, T new_value) {
        return _value.compare_exchange_weak(expected, new_value,
                                            butil::memory_order_relaxed);
    }

    template <typename Op, typename T1>
    void modify(const Op &op, const T1 &value2) {
        T old_value = _value.load(butil::memory_order_relaxed);
        T new_value = old_value;
        call_op_returning_void(op, new_value, value2);
        // There's a contention with the reset operation of combiner,
        // if the tls value has been modified during _op, the
        // compare_exchange_weak operation will fail and recalculation is
        // to be processed according to the new version of value
        while (!_value.compare_exchange_weak(
                   old_value, new_value, butil::memory_order_relaxed)) {
            new_value = old_value;
            call_op_returning_void(op, new_value, value2);
        }
    }

private:
    butil::atomic<T> _value;
};

/**
 * AgentCombiner同样是一个模板，模板参数为结果类型，元素类型和二元操作符， ResultTp和ElementTp分别用于聚合结果和单个tls元素
 * 对于adder这种普通reducer类型的bvar，二者是完全一样的： 
 *     typedef typename detail::AgentCombiner<T, T, Op> combiner_type;
 *     Reducer(typename butil::add_cr_non_integral<T>::type identity = T(),
            const Op& op = Op(),
            const InvOp& inv_op = InvOp())
        : _combiner(identity, identity, op)
        , _sampler(NULL)         // 只有被window之类的追踪了才会去新建sampler，单纯的使用reducer不需要sampler
        , _series_sampler(NULL)
        , _inv_op(inv_op) {
       }
 */
template <typename ResultTp, typename ElementTp, typename BinaryOp>
class AgentCombiner {
public:
    typedef ResultTp result_type;
    typedef ElementTp element_type;
    typedef AgentCombiner<ResultTp, ElementTp, BinaryOp> self_type;
// 申明 GlobalValue<self_type> 为友元类是因为 GlobalValue 要访问 combiner 的私有变量    
friend class GlobalValue<self_type>;
    
    // 对于一个 bvar 变量，每个 tls 保存的值对应链表中的一个 Agent
    // : public butil::LinkNode<Agent> 奇异递归模板模式(Curiously Recurring Template Pattern)，简称 CRTP ，这里这么使用的好处是可以提高运行效率
    // butil::Linknode是一种双向链表节点类型，提供了向前取向后取向前插入向后插入取值赋值等等基本操作，这里Agent通过继承的方式基于链表来进行agent的数据的保存和处理，
    struct Agent : public butil::LinkNode<Agent> {
        Agent() : combiner(NULL) {}

        // 调用 combiner 的 commit_and_erase 来提交现有数据并去除该 agent （从 combiber 维护的 agent 链表里移除)
        ~Agent() {
            if (combiner) {
                combiner->commit_and_erase(this);
                combiner = NULL;
            }
        }
        
        // reset 函数对 combiner 和 element 进行重置
        void reset(const ElementTp& val, self_type* c) {
            combiner = c;
            element.store(val);
        }

        // Call op(GlobalValue<Combiner> &, ElementTp &) to merge tls element
        // into global_result. The common impl. is:
        //   struct XXX {
        //       void operator()(GlobalValue<Combiner> & global_value,
        //                       ElementTp & local_value) const {
        //           if (test_for_merging(local_value)) {
        // 
        //               // Unlock tls element and lock combiner. Obviously
        //               // tls element can be changed during lock().
        //               ResultTp* g = global_value.lock();
        // 
        //               // *g and local_value are not changed provided
        //               // merge_global is called from the thread owning
        //               // the agent.
        //               merge(*g, local_value);
        //
        //               // unlock combiner and lock tls element again.
        //               global_value.unlock();
        //           }
        //
        //           // safe to modify local_value because it's already locked
        //           // or locked again after merging.
        //           ...
        //       }
        //   };
        // 
        // NOTE: Only available to non-atomic types.
        // merge_global 函数用于将当前 agent 里的 tls 值 merge 到 combiner 的 global result 里，部分 bvar 类型需要用到，比如 percentile
        template <typename Op>
        void merge_global(const Op &op) {
            GlobalValue<self_type> g(this, combiner);
            element.merge_global(op, g);
        }

        // combiner指针指向当前 agent 所属 bvar 的 combiner ，初始值为 Null ，可以据此判断此 agent 是否已经分配
        self_type *combiner;
        // element 为实际保存数据的变量。
        ElementContainer<ElementTp> element;
    };

    typedef detail::AgentGroup<Agent> AgentGroup;

    explicit AgentCombiner(const ResultTp result_identity = ResultTp(),
                           const ElementTp element_identity = ElementTp(),
                           const BinaryOp& op = BinaryOp())
        : _id(AgentGroup::create_new_agent())       // _id的赋值调用的是 AgentGroup 的 create_new_agent()函数，分配了一个当前bvar独有的id
        , _op(op)
        , _global_result(result_identity)
        , _result_identity(result_identity)
        , _element_identity(element_identity) {
    }

    ~AgentCombiner() {      // 析构函数 清除 agent 数据然后归还id
        if (_id >= 0) {
            clear_all_agents();
            AgentGroup::destroy_agent(_id);
            _id = -1;
        }
    }
    
    // [Threadsafe] May be called from anywhere
    // 汇聚所有 agent 的值返回，这也是外部最常调用的一个函数，比如 reducer 的 get_value 调用的就是这个函数，
    // 遍历 _agents 链表汇总所有 agent 的 tls 值和 _global_result 保存的值返回
    ResultTp combine_agents() const {
        ElementTp tls_value;
        butil::AutoLock guard(_lock);
        ResultTp ret = _global_result;
        for (butil::LinkNode<Agent>* node = _agents.head();
             node != _agents.end(); node = node->next()) {
            node->value()->element.load(&tls_value);
            call_op_returning_void(_op, ret, tls_value);
        }
        return ret;
    }

    typename butil::add_cr_non_integral<ElementTp>::type element_identity() const 
    { return _element_identity; }
    typename butil::add_cr_non_integral<ResultTp>::type result_identity() const 
    { return _result_identity; }

    // [Threadsafe] May be called from anywhere.
    // 重置所有的 agent ，包括 combiner 内部的 _global_result ，返回汇总值
    // 和 combine_agents 很类似，区别就是会额外重置 agent 值和 _global_result 值
    ResultTp reset_all_agents() {
        ElementTp prev;
        butil::AutoLock guard(_lock);
        ResultTp tmp = _global_result;
        _global_result = _result_identity;
        for (butil::LinkNode<Agent>* node = _agents.head();
             node != _agents.end(); node = node->next()) {
            node->value()->element.exchange(&prev, _element_identity);
            call_op_returning_void(_op, tmp, prev);
        }
        return tmp;
    }

    // Always called from the thread owning the agent.
    // 提交 tls 值，到 _global_result 中，并且移除 agent ，Agent的析构函数会调用这个，
    // 也就是某个线程退出的时候会调用这个函数实现提交完数据后将自己的 agent 从 bvar 的 agent list 里移除。
    void commit_and_erase(Agent *agent) {
        if (NULL == agent) {
            return;
        }
        ElementTp local;
        butil::AutoLock guard(_lock);
        // TODO: For non-atomic types, we can pass the reference to op directly.
        // But atomic types cannot. The code is a little troublesome to write.
        agent->element.load(&local);
        call_op_returning_void(_op, _global_result, local);
        agent->RemoveFromList();
    }

    // Always called from the thread owning the agent
    // 提交 tls 值并且利用 _element_identity 将 tls 置为初值，有些bvar需要用到这个功能。
    void commit_and_clear(Agent *agent) {
        if (NULL == agent) {
            return;
        }
        ElementTp prev;
        butil::AutoLock guard(_lock);
        agent->element.exchange(&prev, _element_identity);
        call_op_returning_void(_op, _global_result, prev);
    }

    // We need this function to be as fast as possible.
    /**
     * 获取或者创建本线程对应的agent，因为需要尽可能快，所以首先调用更快的 AgentGroup::get_tls_agent ，
     * 如果是null才去调用 AgentGroup::get_or_create_tls_agent ，由于只有线程第一次访问没有agent的时候才会是null，
     * 所以大部分情况都能比较快地得到agent，中间判断如果 agent->combiner 非null，说明是先前建立的agent，直接返回即可，
     * 否则需要把当前 combiner 赋给它并且将agent挂到 _agents 链表里面。
     *
     * 每个线程一个对应一个 _id, 以及对应一个 agent right???
     * 
     */
    inline Agent* get_or_create_tls_agent() {
        Agent* agent = AgentGroup::get_tls_agent(_id);  // 传入当前 bvar 对应的 _id, 然后结合当前线程 id ，拿到该 bvar 的 tls 值
        if (!agent) {
            // Create the agent
            agent = AgentGroup::get_or_create_tls_agent(_id);
            if (NULL == agent) {
                LOG(FATAL) << "Fail to create agent";
                return NULL;
            }
        }
        if (agent->combiner) {
            return agent;
        }
        agent->reset(_element_identity, this);
        // TODO: Is uniqueness-checking necessary here?
        {
            butil::AutoLock guard(_lock);
            _agents.Append(agent);                  // 获取新的agent 后， 将其入队 _agents
        }
        return agent;
    }

    // 清除所有 agents ， combiner 的析构函数会调用次函数，因为 agent 有可能被重用，所以对每个 agent 都要 reset 一下
    void clear_all_agents() {
        butil::AutoLock guard(_lock);
        // reseting agents is must because the agent object may be reused.
        // Set element to be default-constructed so that if it's non-pod,
        // internal allocations should be released.
        for (butil::LinkNode<Agent>* 
                node = _agents.head(); node != _agents.end();) {
            node->value()->reset(ElementTp(), NULL);
            butil::LinkNode<Agent>* const saved_next =  node->next();
            node->RemoveFromList();
            node = saved_next;
        }
    }

    const BinaryOp& op() const { return _op; }

    bool valid() const { return _id >= 0; }

private:
    AgentId                                     _id;                // 由 agentgroup 分配的对应当前 bvar 变量的一个 id ，用于去 tls block 里寻址.
    BinaryOp                                    _op;                // 此bvar的操作符
    mutable butil::Lock                          _lock;             // 用于操作全局汇总结果的锁
    ResultTp                                    _global_result;     // 用来保存汇总结果  
    ResultTp                                    _result_identity;   // 本质上分别是 ResultTp 和 ElementTp 类型的初始化过后不经其他修改的变量,
    ElementTp                                   _element_identity;  // 被各种 reset 性质的函数用来清空变量
    butil::LinkedList<Agent>                     _agents;           // 保存了当前 bvar （应该是当前线程吧？？？） 所有 agent 的链表
};

}  // namespace detail
}  // namespace bvar

#endif  // BVAR_COMBINER_H
