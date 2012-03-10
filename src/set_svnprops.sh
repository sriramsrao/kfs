#!/bin/bash

cd $( dirname $0 )

for i in $( find . ../examples -name \*.cpp -or -name \*.cc -or -name \*.h -or -name \*.py -or -name \*.java ); do svn propset svn:keywords Id $i; done | grep -v "property 'svn:keywords' set on"
