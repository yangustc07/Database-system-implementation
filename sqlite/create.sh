#!/bin/bash
#Source: http://www.cs.toronto.edu/~ryanjohn/teaching/cscc43-s11/sqlite+tpc-h+windows-v03.pdf

tblDir=../data/1G
cp ${tblDir}/*.tbl .

for f in *.tbl; do sed --in-place= -e 's/|$//' $f; done
for f in *.tbl; do sqlite3 tpch.db ".import $f $(basename $f .tbl)"; done
