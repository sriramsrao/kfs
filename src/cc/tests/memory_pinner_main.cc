//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: memory_pinner_main.cc $
//
// Created 2011/03/07
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// Yahoo PROPRIETARY and CONFIDENTIAL.
//
// \brief Tool that pins down main memory.  This provides a simple way 
// to lock down the memory and reduce memory available for buffer cache.
//----------------------------------------------------------------------------

#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <cerrno>
#include <boost/lexical_cast.hpp>

using std::cout;
using std::endl;

int main(int argc, char **argv)
{
    off_t memSizeToPin;
    void *bufStart;

    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <mem size to pin>" << endl;
        exit(0);
    }
    memSizeToPin = boost::lexical_cast<off_t>(argv[1]);

    bufStart = mmap(NULL, memSizeToPin, PROT_READ|PROT_WRITE, 
        MAP_PRIVATE|MAP_ANON|MAP_NORESERVE, -1, 0);
    if (bufStart == NULL) {
        cout << "Unable to mmap memory: error = " << errno << endl;
        exit(-1);
    }
    if (mlock(bufStart, memSizeToPin) < 0) {
        cout << "Unable to lock memory: error = " << errno << " which is: " 
            << strerror(errno) << endl;
        exit(-1);
    }
    // Set this to some value so that the page is really there
    char *dataBuf = (char *) bufStart;
    for (off_t i = 0; i < memSizeToPin; i++) {
        dataBuf[i] = 'a';
    }
    cout << "Got the memory...hit cntrl-c to exit" << endl;
    while (1) {
        sleep(60);
    }
}

