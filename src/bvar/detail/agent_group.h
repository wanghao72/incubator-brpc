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

// Date 2014/09/24 19:34:24

#ifndef  BVAR_DETAIL__AGENT_GROUP_H
#define  BVAR_DETAIL__AGENT_GROUP_H

#include <pthread.h>                        // pthread_mutex_*
#include <stdlib.h>                         // abort

#include <new>                              // std::nothrow
#include <deque>                            // std::deque
#include <vector>                           // std::vector

#include "butil/errno.h"                     // errno
#include "butil/thread_local.h"              // thread_atexit
#include "butil/macros.h"                    // BAIDU_CACHELINE_ALIGNMENT
#include "butil/scoped_lock.h"
#include "butil/logging.h"

namespace bvar {
namespace detail {

typedef int AgentId;  // 类外定义了AgentId，其实就是个int，用来标识变量、在存储块中定位变量位置的

// General NOTES:
// * Don't use bound-checking vector::at.
// * static functions in template class are not guaranteed to be inlined,
//   add inline keyword explicitly.
// * Put fast path in "if" branch, which is more cpu-wise.
// * don't use __builtin_expect excessively because CPU may predict the branch
//   better than you. Only hint branches that are definitely unusual.
//   
//   类名是AgentGroup，之所以叫group是因为同类型的tls数据会统一管理，模板参数是Agent，使用上传入的是AgentCombiner::Agent包装下的实际类型，
//   比如bvar::Adder value1和bvar::Adder value2所用的会是相同实例化的类，会共用同一个tls存储数组变量。

template <typename Agent>
class AgentGroup {
public:
    typedef Agent   agent_type;

    // TODO: We should remove the template parameter and unify AgentGroup
    // of all bvar with a same one, to reuse the memory between different
    // type of bvar. The unified AgentGroup allocates small structs in-place
    // and large structs on heap, thus keeping batch efficiencies on small
    // structs and improving memory usage on large structs.
    // 首先是有两个const static的关于每个block存多少个元素的变量，RAW_BLOCK_SIZE 和 ELEMENTS_PER_BLOCK ，
    // 根据赋值我们可以知道，如果Agent类型大于等于4096，那么每个块就一个元素，再比如如果Agent类型占1024，那么每块的元素会是4个
    const static size_t RAW_BLOCK_SIZE = 4096;
    const static size_t ELEMENTS_PER_BLOCK =
        (RAW_BLOCK_SIZE + sizeof(Agent) - 1) / sizeof(Agent);

    // The most generic method to allocate agents is to call ctor when
    // agent is needed, however we construct all agents when initializing
    // ThreadBlock, which has side effects:
    //  * calling ctor ELEMENTS_PER_BLOCK times is slower.
    //  * calling ctor of non-pod types may be unpredictably slow.
    //  * non-pod types may allocate space inside ctor excessively.
    //  * may return non-null for unexist id.
    //  * lifetime of agent is more complex. User has to reset the agent before
    //    destroying id otherwise when the agent is (implicitly) reused by
    //    another one who gets the reused id, things are screwed.
    // TODO(chenzhangyi01): To fix these problems, a method is to keep a bitmap
    // along with ThreadBlock* in _s_tls_blocks, each bit in the bitmap
    // represents that the agent is constructed or not. Drawback of this method
    // is that the bitmap may take 32bytes (for 256 agents, which is common) so
    // that addressing on _s_tls_blocks may be slower if identifiers distribute
    // sparsely. Another method is to put the bitmap in ThreadBlock. But this
    // makes alignment of ThreadBlock harder and to address the agent we have
    // to touch an additional cacheline: the bitmap. Whereas in the first
    // method, bitmap and ThreadBlock* are in one cacheline.
    // 
    // ThreadBlock是内部struct，则是真正的tls存储类型，由 ELEMENTS_PER_BLOCK 大小的数组 _agents 和一个取指定偏移量位置变量的 at 函数组成
    // 用 BAIDU_CACHELINE_ALIGNMENT 修饰是为了对齐cache line避免 cache bouncing
    struct BAIDU_CACHELINE_ALIGNMENT ThreadBlock {
        inline Agent* at(size_t offset) { return _agents + offset; };
        
    private:
        Agent _agents[ELEMENTS_PER_BLOCK];
    };

    /**
     * 这是新建 agent 的，返回的是新建 agent 的id，这个函数由 combiner 的构造函数调用，
     * 就是加锁后先判断是否有空闲的id（包括原来分配的tls存储），有就直接返回老的，
     * 否则返回 _s_agent_kinds 的值并对 _s_agent_kinds 自增，也就是新建了一个id，
     * 这里说的新建仅仅是新建id，并没有分配空间构造变量，如果是已有的id则是原来已经构造好的。
     * 
     */
    inline static AgentId create_new_agent() {
        BAIDU_SCOPED_LOCK(_s_mutex);
        AgentId agent_id = 0;
        if (!_get_free_ids().empty()) {
            agent_id = _get_free_ids().back();
            _get_free_ids().pop_back();
        } else {
            agent_id = _s_agent_kinds++;
        }
        return agent_id;
    }

    /**
     * 销毁 agent ，这里说的销毁也可以理解成归还，并没有delete建立的对象，后续还可以重用，
     * combiner 的析构函数会调用，比如一个 bvar 析构了，对应的 combiner 析构了也就会归还 agentid
     */
    inline static int destroy_agent(AgentId id) {
        // TODO: How to avoid double free?
        BAIDU_SCOPED_LOCK(_s_mutex);
        if (id < 0 || id >= _s_agent_kinds) {
            errno = EINVAL;
            return -1;
        }
        _get_free_ids().push_back(id);
        return 0;
    }

    /**
     * 根据 id 拿到 agent 指针，如果 block 还没分配直接返回NULL，id除以 perlock 得到 block_id （定位到 block ）
     * 再根据块内偏移 id - blockid * elements_per_block 定位到具体数据.
     * 注意注释里描述的，不存在的id可能返回非null.是因为id和对应的数据是可以重用的，比如id=10的归还了，但因为10所指向的数据块还在，这个时候用如果用10来取仍然能取到，不过实际使用中没啥影响。
     */
    // Note: May return non-null for unexist id, see notes on ThreadBlock
    // We need this function to be as fast as possible.
    inline static Agent* get_tls_agent(AgentId id) {
        if (__builtin_expect(id >= 0, 1)) {
            if (_s_tls_blocks) {
                const size_t block_id = (size_t)id / ELEMENTS_PER_BLOCK;
                if (block_id < _s_tls_blocks->size()) {
                    ThreadBlock* const tb = (*_s_tls_blocks)[block_id];
                    if (tb) {
                        return tb->at(id - block_id * ELEMENTS_PER_BLOCK);
                    }
                }
            }
        }
        return NULL;
    }

    /**
     * 和 get_tls_agent 类似，根据id拿到agent指针，但如果block还没分配会进行分配，之所以分成了两个函数是为了让 get_tls_agent 的部分实现尽可能的快。
     * Combiner在调用的时候会先调 AgentGroup::get_tls_agent(_id),如果为null再调 AgentGroup::get_or_create_tls_agent(_id). 
     * 容易理解实际使用中 get_tls_agent 就能返回非NULL的占比很大。和 get_tls_agent 的主要区别在于
     * 多了 _s_tls_blocks 为null和 (*_s_tls_blocks)[block_id] 为null的时候的空间分配，也就是or_create的含义。
     * 注意中间的 butil::thread_atexit(_destroy_tls_blocks),线程退出的时候调用上面说的 private 的 _destroy_tls_blocks 函数。
     */
    // Note: May return non-null for unexist id, see notes on ThreadBlock
    inline static Agent* get_or_create_tls_agent(AgentId id) {
        if (__builtin_expect(id < 0, 0)) {
            CHECK(false) << "Invalid id=" << id;
            return NULL;
        }
        if (_s_tls_blocks == NULL) {
            _s_tls_blocks = new (std::nothrow) std::vector<ThreadBlock *>;
            if (__builtin_expect(_s_tls_blocks == NULL, 0)) {
                LOG(FATAL) << "Fail to create vector, " << berror();
                return NULL;
            }
            butil::thread_atexit(_destroy_tls_blocks);
        }
        const size_t block_id = (size_t)id / ELEMENTS_PER_BLOCK; 
        if (block_id >= _s_tls_blocks->size()) {
            // The 32ul avoid pointless small resizes.
            _s_tls_blocks->resize(std::max(block_id + 1, 32ul));
        }
        ThreadBlock* tb = (*_s_tls_blocks)[block_id];
        if (tb == NULL) {
            ThreadBlock *new_block = new (std::nothrow) ThreadBlock;
            if (__builtin_expect(new_block == NULL, 0)) {
                return NULL;
            }
            tb = new_block;
            (*_s_tls_blocks)[block_id] = new_block;
        }
        return tb->at(id - block_id * ELEMENTS_PER_BLOCK);
    }

private:
    // 析构 _s_tls_blocks 里的元素并 delete _s_tls_blocks 本身，用于线程退出的时候清除tls存储。
    static void _destroy_tls_blocks() {
        if (!_s_tls_blocks) {
            return;
        }
        for (size_t i = 0; i < _s_tls_blocks->size(); ++i) {
            delete (*_s_tls_blocks)[i];
        }
        delete _s_tls_blocks;
        _s_tls_blocks = NULL;
    }


    // 获取空闲的已有 id ，用 deque 保存我理解是因为它resize比较高效。
    inline static std::deque<AgentId> &_get_free_ids() {
        if (__builtin_expect(!_s_free_ids, 0)) {
            _s_free_ids = new (std::nothrow) std::deque<AgentId>();
            if (!_s_free_ids) {
                abort();
            }
        }
        return *_s_free_ids;
    }

    // 四个static变量，因为是 static 变量模板参数相同的实例都会共用
    // ！！！ 因为是 static 的，所以当前线程就这一个 vector<> ，保存当前线程的所有 bvar
    static pthread_mutex_t                      _s_mutex;        // _s_mutex 是新建和销毁 agent 要用到的锁
    static AgentId                              _s_agent_kinds;  // _s_agent_kinds 是当前 agentgroup （Agent参数相同）的 agent 数量，同时也用于构造agentId
    static std::deque<AgentId>                  *_s_free_ids;    // _s_free_ids 是个 deque 的指针，保存了空闲的 agentId 用于再分配
    static __thread std::vector<ThreadBlock *>  *_s_tls_blocks;  // _s_tls_blocks 由 __thread 修饰， tls 变量，是个 vector 的指针，
                                                // 这个 vector 保存的则是 ThreadBlock 的指针，这也是 agent 的核心变量，指向每个 thread 的 tls 数据块
};

template <typename Agent>
pthread_mutex_t AgentGroup<Agent>::_s_mutex = PTHREAD_MUTEX_INITIALIZER;

template <typename Agent>
std::deque<AgentId>* AgentGroup<Agent>::_s_free_ids = NULL;

template <typename Agent>
AgentId AgentGroup<Agent>::_s_agent_kinds = 0;

template <typename Agent>
__thread std::vector<typename AgentGroup<Agent>::ThreadBlock *>
*AgentGroup<Agent>::_s_tls_blocks = NULL;

}  // namespace detail
}  // namespace bvar

#endif  //BVAR_DETAIL__AGENT_GROUP_H
