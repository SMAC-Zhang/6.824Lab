#!/usr/bin/bash
for ((i=0;i<500;++i))
do  
    echo "test $i round"
    go test -run 2A
    if (($? != 0)); then
        echo "failed test at $i round"
        break
    fi
done

for ((i=0;i<500;++i))
do  
    echo "test $i round"
    go test -run 2B
    if (($? != 0)); then
        echo "failed test at $i round"
        break
    fi
done