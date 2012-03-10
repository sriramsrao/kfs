//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsIndexReader_main.cc 1727 2011-01-26 20:14:40Z sriramr $
//
// Created 2011/01/10
//
// Copyright 2011 Yahoo Corporation.  All rights reserved.
// Yahoo PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <fstream>
#include <time.h>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "libkfsClient/KfsClient.h"
#include "common/log.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

using namespace KFS;
namespace po = boost::program_options;

KfsClientPtr gKfsClient;

int
main(int argc, char **argv)
{
    ServerLocation metaLoc;
    string filename;
    string outputDir;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("metahost,M", po::value<string>(&metaLoc.hostname)->default_value("localhost"), "KFS metaserver hostname")
        ("metaport,P", po::value<int>(&metaLoc.port)->default_value(20000), "KFS metaserver port")
        ("outputdir,O", po::value<string>(&outputDir)->default_value(""), "Dir where indexes should be saved")
        ("file,f", po::value<string>(&filename)->default_value("/jobs"), "File for which indexes have to be read")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);    
    
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    gKfsClient = getKfsClientFactory()->GetClient(metaLoc.hostname, metaLoc.port);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }
    gKfsClient->SetLogLevel("DEBUG");
    int kfsFd = gKfsClient->Open(filename.c_str(), O_RDONLY);
    if (kfsFd < 0) {
        cout << "Unable to open: " << filename << endl;
        exit(-1);
    }

    struct stat statRes;

    if (gKfsClient->Stat(filename.c_str(), statRes) < 0) {
        cout << "Unable to stat: " << filename << endl;
        exit(-1);
    }

    off_t currPos = 0;
    while (currPos < statRes.st_size) {
        char *buffer = NULL;
        int indexSize = 0;
        int res = gKfsClient->GetChunkIndex(kfsFd, &buffer, indexSize);
        if (res < 0)
            break;
        cout << "@offset: " << currPos << " Got chunk index of size: " << indexSize << endl;
        if (outputDir != "") {
            string fn = outputDir + "/" + boost::lexical_cast<string>(currPos) + ".idx";
            int ofd = open(fn.c_str(), O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
            if (ofd != -1) {
                write(ofd, buffer, indexSize);
                close(ofd);
            }
        }
        delete [] buffer;
        currPos += KFS::CHUNKSIZE;
        gKfsClient->Seek(kfsFd, currPos, SEEK_SET);
    }
    gKfsClient->Close(kfsFd);
}

