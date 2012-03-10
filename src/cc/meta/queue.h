/*!
 * $Id: queue.h 1552 2011-01-06 22:21:54Z sriramr $
 *
 * \file queue.h
 * \brief queue for requested metadata operations
 *
 * Copyright 2008 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#if !defined(KFS_QUEUE_H)
#define KFS_QUEUE_H

#include <deque>
#include <algorithm>
#include <functional>

#include "thread.h"

using std::deque;
using std::unary_function;

namespace KFS {

/*!
 * \brief mechanism to apply algorithm on MetaQueue
 * 
 * To execute algorithms, such as, for_each on the MetaQueue, we
 * need an object that defines the () operator.  The FunctorBase
 * is the abstract base class that provides such a definition.  Users
 * of the MetaQueue need to define a derived class of FunctorBase
 * that implements the () operator.
*/

template <typename T> class FunctorBase {
public:
	FunctorBase() { }
	virtual void operator() (T *arg) = 0;
};

/*
 * \brief a wrapper around the functor base
 *
 * for_each(start, end, f) does: f(element).  The type of "f" is
 * passed in by value.  So, if f is a derived type, the "wrong" operator ()
 * will get invoked.  What we really need is to find the right virtual function.
 * So, put the functor base into the functor wrapper; pass the functor wrapper
 * to for_each; for_each will call the operator defined here and we then call
 * the "right" virtual function.  Sigh...
*/
template <typename T> class FunctorWrapper {
	FunctorBase<T> *func;
public:
	FunctorWrapper(FunctorBase<T> *f) : func(f) { }
	void operator () (T *arg) {
		(*func)(arg);
	}
};

/*
 * Same idea as the functor wrappers, this time for predicates
*/
template <typename T> class PredBase {
public:
	PredBase() { }
	virtual bool operator() (T *arg) = 0;
};

template <typename T> class PredWrapper {
	PredBase<T> *func;
public:
	PredWrapper(PredBase<T> *f) : func(f) { }
	bool operator () (T *arg) {
		return (*func)(arg);
	}
};


/*!
 * \brief deque with mutex for threaded access
 *
 * We use an STL deque to keep the list of pending requests;
 * throw in a mutex to allow multithreaded updating.
 */
template <typename T> class MetaQueue {
public:
	typedef deque<T *> Queue;
	MetaQueue(): waiters(0) { }
	~MetaQueue() { }
	bool empty() { return queue.empty(); }
	void enqueue(T *r);
	T *dequeue();
	T *dequeue_nowait();
	void apply(FunctorWrapper<T> &f);
	void remove(PredWrapper<T> &f);
        void swap(Queue &q);
private:
	Queue queue;
	MetaThread thread;
	int waiters;		//!< threads waiting for results
	T *dequeue_internal();
private:
	MetaQueue(const MetaQueue<T>&);
	MetaQueue<T>& operator=(const MetaQueue<T>&);
};

/*!
 * \brief add a request to the queue
 * \param[in] req the request to be queued.
 *
 * Restarts waiting threads if the queue was empty.
 */
template <typename T> void
MetaQueue <T>::enqueue(T *req)
{
	thread.lock();

	if (waiters != 0 || queue.empty())
		thread.wakeup();

	queue.push_back(req);
	thread.unlock();
}

/*!
 * \brief remove a request from the queue without locking
 * \return frontmost (oldest) request on queue
 *
 * Common code for dequeue and dequeue_nowait; for internal use only.
 */
template <typename T> T *
MetaQueue <T>::dequeue_internal()
{
	while (queue.empty())
		thread.sleep();

	T *r = queue.front();
	queue.pop_front();
	return r;
}

/*!
 * \brief remove a request from the queue
 * \return frontmost (oldest) request on queue
 *
 * Blocks while the queue is empty.
 */
template <typename T> T *
MetaQueue <T>::dequeue()
{
	thread.lock();
	T *r = dequeue_internal();
	thread.unlock();
	return r;
}

/*!
 * \brief remove a request from the queue
 * \return frontmost (oldest) request on queue or NULL, if queue is empty
 *
 * Does not block.
 */
template <typename T> T *
MetaQueue <T>::dequeue_nowait()
{
	thread.lock();
	T *r = (queue.empty()) ? NULL : dequeue_internal();
	thread.unlock();
	return r;
}

/*!
 * \brief apply a functor to each element of the deque
*/
template <typename T> void
MetaQueue <T>::apply(FunctorWrapper<T> &f)
{
	thread.lock();
	for_each(queue.begin(), queue.end(), f);
	thread.unlock();
}

/*!
 * \brief remove elements from the queue based on predicate
*/
template <typename T> void
MetaQueue <T>::remove(PredWrapper<T> &f)
{
	thread.lock();
	remove_if(queue.begin(), queue.end(), f);
	thread.unlock();
}

/*!
 * \brief swap
*/
template <typename T> void
MetaQueue <T>::swap(Queue &q)
{
	thread.lock();
	queue.swap(q);
	thread.unlock();
}

}
#endif /* !defined(KFS_QUEUE_H) */
