/* Copyright 2021 Errord Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef THREAD_H_
#define THREAD_H_

#include <pthread.h>
#include <tasks.h>

namespace textsort {

class Task;
class ThreadPool;

class ThreadLock {
public:
  class Sync {
  public:
    Sync(ThreadLock* lock) : lock_(lock) {
      lock_->Lock();
    };
    ~Sync() {
      lock_->Unlock();
    };
  private:
    ThreadLock* lock_;
  };

public:
  ThreadLock() : mutex_(nullptr), is_lock_(false) {
    mutex_ = new pthread_mutex_t;
    int rc = pthread_mutex_init(mutex_, nullptr);
    assert(rc == 0);
  };
  ~ThreadLock() {
    if (mutex_ != nullptr) {
      pthread_mutex_destroy(mutex_);
      delete mutex_;
      mutex_ = nullptr;
    }
  };
  int Lock() {
    assert(mutex_ != nullptr);
    int rc = pthread_mutex_lock(mutex_);
    assert(rc == 0);
    is_lock_ = true;
    return rc;
  };
  int Unlock() {
    assert(mutex_ != nullptr);
    is_lock_ = false;
    int rc = pthread_mutex_unlock(mutex_);
    assert(rc == 0);
    return rc;
  };
  bool IsLock() {
    return is_lock_;
  }
  Sync GetSync() {
    Sync sync(this);
    return sync;
  };
  pthread_mutex_t* GetMutex() const {
    return mutex_;
  };
private:
  pthread_mutex_t* mutex_;
  bool is_lock_;
};

class ThreadWait: public ThreadLock {
public:
  ThreadWait() {
    int rc = pthread_cond_init(&cond_, nullptr);
    assert(rc == 0);
  };
  ~ThreadWait() {
    int rc = pthread_cond_destroy(&cond_);
    assert(rc == 0);
  };
  int Wait() {
    int rc = pthread_cond_wait(&cond_, GetMutex());
    return rc;
  };
  int Notify() {
    int rc = pthread_cond_signal(&cond_);
    return rc;
  };
  int NotifyAll() {
    int rc = pthread_cond_broadcast(&cond_);
    return rc;
  };
private:
  pthread_cond_t cond_;
};

class TaskThread {
public:
  enum ThreadState {
    Init,
    PreStart,
    Running,
    Done
  };
public:
  TaskThread(ThreadPool* pool, int pool_id) : TaskThread(pool, pool_id, nullptr) {};
  TaskThread(ThreadPool* pool, int pool_id, LinkList<Task*>* tasks) 
    : pool_(pool), pool_id_(pool_id), tasks_(tasks),
      next_thread_ptr_(nullptr), state_(TaskThread::Init) {
    tasks_->ForEach([this](Task* task) {
      task->SetThread(this);
    });
  };
  ~TaskThread() {
    ReleaseResource();
  };
  TaskThread* NextThread() {
    return next_thread_ptr_;
  };
  void SetNextThread(TaskThread* thread_ptr) {
    next_thread_ptr_ = thread_ptr;
  };
  pthread_t GetThreadId() const {
    return pthread_self();
  };
  pthread_t PthreadId() const {
    return pthread_id;
  };
  int ThreadPoolId() const {
    return pool_id_;
  };
  ThreadPool* Pool() {
    return pool_;
  };
  bool IsInitial() const {
    return state_ == ThreadState::Init;
  };
  bool IsRunning() const {
    return state_ == ThreadState::PreStart || state_ == ThreadState::Running;
  };
  void SetThreadState(ThreadState state) {
    state_ = state;
  };
  bool StartThread();
  void RunTask(); // called by a thread
  void ReleaseResource() {
    if (tasks_ != nullptr) {
      tasks_->ForEach([](Task* task) {
        delete task;
      });
      delete tasks_;
      tasks_ = nullptr;
    }
  };
private:
  ThreadPool* pool_;
  LinkList<Task*>* tasks_;
  TaskThread* next_thread_ptr_;
  pthread_t pthread_id;
  int pool_id_;
  ThreadState state_;
};

class ThreadPool {
public:
  ThreadPool()
    : thread_head_ptr_(nullptr), thread_tail_ptr_(nullptr),
      pool_max_id_(0) {};
  ~ThreadPool() {
    Close();
  };
  ThreadLock& LogLock() {
    return log_lock_;
  };
  TaskThread* AddTask(Task* task);
  TaskThread* AddTask(LinkList<Task*>* tasks);
  // Run a task now
  void RunTask(Task* task);
  void RunTask(LinkList<Task*>* tasks);
  void RunTasks();
  void WaitTaskDone();
private:
  void Close();
  TaskThread* thread_head_ptr_;
  TaskThread* thread_tail_ptr_;
  ThreadLock log_lock_;
  int pool_max_id_;
};

template <typename L>
void LockLambda(ThreadLock& lock, L lambda) {
	lock.Lock();
  lambda();
  lock.Unlock();
};

#define USE_THREAD(D) TaskThread* thread = D

#define LOG_LOCK(LOG) {             \
    thread->Pool()->LogLock().Lock();       \
    LOG;                                    \
    thread->Pool()->LogLock().Unlock();     \
  } while (0)

} // namespace textsort

#endif // THREAD_H_
