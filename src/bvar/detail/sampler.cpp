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

// 采集器，像window等和时间窗口相关的类型需要使用 sampler 来定时采集，每一个需要采集的 bvar 对应一个 sampler
/**
（1） Sample ：用于保存采样值的结构体。
（2） SamplerCollector ：全局的用来遍历所有的Sampler并执行采样操作的结构。
（3） Sampler ：采样器的基类
（4） ReducerSampler ：继承自Sampler的用于对Reducer类型的bvar进行采样的采样器
 */

// Date: Tue Jul 28 18:14:40 CST 2015

#include "butil/time.h"
#include "butil/memory/singleton_on_pthread_once.h"
#include "bvar/reducer.h"
#include "bvar/detail/sampler.h"
#include "bvar/passive_status.h"
#include "bvar/window.h"

namespace bvar {
namespace detail {

const int WARN_NOSLEEP_THRESHOLD = 2;

// Combine two circular linked list into one.
// CombineSampler 运算符，做了一个合并，也就是把s1插到s2之前
struct CombineSampler {
    void operator()(Sampler* & s1, Sampler* s2) const {
        if (s2 == NULL) {
            return;
        }
        if (s1 == NULL) {
            s1 = s2;
            return;
        }
        s1->InsertBeforeAsList(s2);
    }
};

// True iff pthread_atfork was called. The callback to atfork works for child
// of child as well, no need to register in the child again.
static bool registered_atfork = false;

// Call take_sample() of all scheduled samplers.
// This can be done with regular timer thread, but it's way too slow(global
// contention + log(N) heap manipulations). We need it to be super fast so that
// creation overhead of Window<> is negliable.
// The trick is to use Reducer<Sampler*, CombineSampler>. Each Sampler is
// doubly linked, thus we can reduce multiple Samplers into one cicurlarly
// doubly linked list, and multiple lists into larger lists. We create a
// dedicated thread to periodically get_value() which is just the combined
// list of Samplers. Waking through the list and call take_sample().
// If a Sampler needs to be deleted, we just mark it as unused and the
// deletion is taken place in the thread as well.
// 
// 这个类是一个用来遍历所有的Sampler并执行采样操作的全局结构
// 对于需要定期执行的操作，一种常见的做法是用定时器线程，但一旦sampler多了，效率上是不尽人意的，
// 这里把 SamplerCollector 定义成模板参数为 Sampler 指针和一个特定运算符 CombineSampler 的Reducer
class SamplerCollector : public bvar::Reducer<Sampler*, CombineSampler> {
public:
    SamplerCollector()
        : _created(false)
        , _stop(false)
        , _cumulated_time_us(0) {
        create_sampling_thread();
    }
    ~SamplerCollector() {
        if (_created) {
            _stop = true;
            pthread_join(_tid, NULL);
            _created = false;
        }
    }

private:
    // Support for fork:
    // * The singleton can be null before forking, the child callback will not
    //   be registered.
    // * If the singleton is not null before forking, the child callback will
    //   be registered and the sampling thread will be re-created.
    // * A forked program can be forked again.

    static void child_callback_atfork() {
        butil::get_leaky_singleton<SamplerCollector>()->after_forked_as_child();
    }

    void create_sampling_thread() {
        const int rc = pthread_create(&_tid, NULL, sampling_thread, this);
        if (rc != 0) {
            LOG(FATAL) << "Fail to create sampling_thread, " << berror(rc);
        } else {
            _created = true;
            if (!registered_atfork) {
                registered_atfork = true;
                pthread_atfork(NULL, NULL, child_callback_atfork);
            }
        }
    }

    void after_forked_as_child() {
        _created = false;
        create_sampling_thread();
    }

    void run();

    static void* sampling_thread(void* arg) {
        static_cast<SamplerCollector*>(arg)->run();
        return NULL;
    }

    static double get_cumulated_time(void* arg) {
        return static_cast<SamplerCollector*>(arg)->_cumulated_time_us / 1000.0 / 1000.0;
    }

private:
    bool _created;
    bool _stop;
    int64_t _cumulated_time_us;
    pthread_t _tid;
};

#ifndef UNIT_TEST
static PassiveStatus<double>* s_cumulated_time_bvar = NULL;
static bvar::PerSecond<bvar::PassiveStatus<double> >* s_sampling_thread_usage_bvar = NULL;
#endif

// 调用 pthread_create 启动线程执行 sampling_thread() ，实质上就是调用run()
void SamplerCollector::run() {
#ifndef UNIT_TEST
    // PassiveStatus 类型的 cumulated_time 和使用它的 usage 则是用来监控执行时间的
    // NOTE:
    // * Following vars can't be created on thread's stack since this thread
    //   may be adandoned at any time after forking.
    // * They can't created inside the constructor of SamplerCollector as well,
    //   which results in deadlock.
    if (s_cumulated_time_bvar == NULL) {
        s_cumulated_time_bvar =
            new PassiveStatus<double>(get_cumulated_time, this);
    }
    if (s_sampling_thread_usage_bvar == NULL) {
        s_sampling_thread_usage_bvar =
            new bvar::PerSecond<bvar::PassiveStatus<double> >(
                    "bvar_sampler_collector_usage", s_cumulated_time_bvar, 10);
    }
#endif
    // Root 是整个collector的根节点，也就是串联起来的sampler的起点
    // consecutive_nosleep 是保存连续没有sleep的次数，设定是每隔一秒采集一次，如果一秒内都没有执行完所有的sampler这个值就会加1，表明延迟了。
    butil::LinkNode<Sampler> root;
    int consecutive_nosleep = 0;
    // 只要_stop不为true就一直执行定时采集任务
    while (!_stop) {
        int64_t abstime = butil::gettimeofday_us();

        // 首先是调用reset拿到一个sampler的指针s，SamplerCollector作为一个Reducer，reset实质上调用的是combiner的reset
        // 根据CombineSampler运算符不难理解，就是把所有的Sampler串起来返回后并重置为NULL，随后将这些sampler指针插入到root节点后面，
        // 简单来说就是每次采样前会将新增的sampler加到队列里。
        Sampler* s = this->reset();
        if (s) {
            s->InsertBeforeAsList(&root);
        }

        // 随后则是遍历链表进行采样，需要注意的是会有加锁操作，因为需要和sampler其他的诸如destroy的函数互斥，
        // 然后判断s->_used ，如果为false，说明已经执行了 destroy ，从链表移除并delete，否则采样后解锁
        int nremoved = 0;
        int nsampled = 0;
        for (butil::LinkNode<Sampler>* p = root.next(); p != &root;) {
            // We may remove p from the list, save next first.
            butil::LinkNode<Sampler>* saved_next = p->next();
            Sampler* s = p->value();
            s->_mutex.lock();
            if (!s->_used) {
                s->_mutex.unlock();
                p->RemoveFromList();
                delete s;
                ++nremoved;
            } else {
                s->take_sample();
                s->_mutex.unlock();
                ++nsampled;
            }
            p = saved_next;
        }

        // 一轮采样完成后则是耗时的计算和判断，简单来说就是以函数开头获取的时间为基准时间，纪录本轮耗时，
        // 并判断是否超过1s，如果没超过，sleep掉剩余的部分，
        // 如果超过了，consecutive_nosleep加1，表明本次出现了延迟，不会sleep而是直接进入了下一轮采集，
        // 并且如果达到阈值会打warning日志，没超过则consecutive_nosleep清0
        bool slept = false;
        int64_t now = butil::gettimeofday_us();
        _cumulated_time_us += now - abstime;
        abstime += 1000000L;
        while (abstime > now) {
            ::usleep(abstime - now);
            slept = true;
            now = butil::gettimeofday_us();
        }
        if (slept) {
            consecutive_nosleep = 0;
        } else {            
            if (++consecutive_nosleep >= WARN_NOSLEEP_THRESHOLD) {
                consecutive_nosleep = 0;
                LOG(WARNING) << "bvar is busy at sampling for "
                             << WARN_NOSLEEP_THRESHOLD << " seconds!";
            }
        }
    }
}

Sampler::Sampler() : _used(true) {}

Sampler::~Sampler() {}

void Sampler::schedule() {
    *butil::get_leaky_singleton<SamplerCollector>() << this;
}

void Sampler::destroy() {
    _mutex.lock();
    _used = false;
    _mutex.unlock();
}

}  // namespace detail
}  // namespace bvar
