//
// Copyright (C) 2008 Maciej Sobczak
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_SOURCE
#include "soci/connection-pool.h"
#include "soci/error.h"
#include "soci/session.h"
#include <vector>
#include <utility>

#include <errno.h>
#include <boost/fiber/all.hpp>

using namespace soci;

struct connection_pool::connection_pool_impl
{
    bool find_free(std::size_t & pos)
    {
        for (std::size_t i = 0; i != sessions_.size(); ++i)
        {
            if (sessions_[i].first)
            {
                pos = i;
                return true;
            }
        }

        return false;
    }

    // by convention, first == true means the entry is free (not used)
    std::vector<std::pair<bool, session *> > sessions_;
    boost::fibers::mutex mtx_;
    boost::fibers::condition_variable cond_;
};

connection_pool::connection_pool(std::size_t size)
{
    if (size == 0)
    {
        throw soci_error("Invalid pool size");
    }

    pimpl_ = new connection_pool_impl();
    pimpl_->sessions_.resize(size);
    for (std::size_t i = 0; i != size; ++i)
    {
        pimpl_->sessions_[i] = std::make_pair(true, new session());
    }
}

connection_pool::~connection_pool()
{
    for (std::size_t i = 0; i != pimpl_->sessions_.size(); ++i)
    {
        delete pimpl_->sessions_[i].second;
    }

    delete pimpl_;
}

bool connection_pool::try_lease(std::size_t & pos, int timeout)
{
    boost::fibers::cv_status cc = boost::fibers::cv_status::no_timeout;
    std::unique_lock<boost::fibers::mutex> lk(pimpl_->mtx_);
    while (pimpl_->find_free(pos) == false)
    {
        if (timeout < 0)
        {
            // no timeout, allow unlimited blocking
            pimpl_->cond_.wait(lk);
        }
        else
        {
            // wait with timeout
             cc = pimpl_->cond_.wait_for(lk, std::chrono::milliseconds(timeout));
        }

        if (cc == boost::fibers::cv_status::timeout)
        {
            break;
        }
    }

    if (cc == boost::fibers::cv_status::no_timeout)
    {
        pimpl_->sessions_[pos].first = false;
    }

    lk.unlock();

    if (cc != boost::fibers::cv_status::no_timeout)
    {
        // we can only fail if timeout expired
        if (timeout < 0)
        {
            throw soci_error("Getting connection from the pool unexpectedly failed");
        }

        return false;
    }

    return true;
}

void connection_pool::give_back(std::size_t pos)
{
    if (pos >= pimpl_->sessions_.size())
    {
        throw soci_error("Invalid pool position");
    }

    std::unique_lock<boost::fibers::mutex> lk(pimpl_->mtx_);
    if (pimpl_->sessions_[pos].first)
    {
        lk.unlock();
        throw soci_error("Cannot release pool entry (already free)");
    }

    pimpl_->sessions_[pos].first = true;

    lk.unlock();
    pimpl_->cond_.notify_one();
}


session & connection_pool::at(std::size_t pos)
{
    if (pos >= pimpl_->sessions_.size())
    {
        throw soci_error("Invalid pool position");
    }

    return *(pimpl_->sessions_[pos].second);
}

std::size_t connection_pool::lease()
{
    // dummy default value avoids compiler warning, never leaks to client
    std::size_t pos(0);

    // no timeout, so can't fail
    try_lease(pos, -1);

    return pos;
}


