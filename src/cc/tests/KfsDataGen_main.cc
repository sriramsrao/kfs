
//
// Generate out test data to write/read from KFS and validate for correctness.
//
#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>

using std::cout;
using std::endl;
using std::ofstream;

void generateData(char *buf, int numBytes);
int
main(int argc, char **argv)
{
    int fsize;

    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <filename> <size> " << endl;
        exit(0);
    }
    fsize = atoi(argv[2]);
    generateData(argv[1], fsize);
    
}

void
generateData(char *fname, int numBytes)
{
    ofstream ofs(fname);
    int i;

    if (!ofs) {
        cout << "Unable to open file: " << fname << endl;
        return;
    }
    
    for (i = 0; i < numBytes; i++) {
        ofs << (char) ('a' + (rand() % 26));
    }
    ofs.close();
}
    
    
