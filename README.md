This is the final version of the Data profiling Tool. It has been created using Extensive use of Unix shell scripting language. The tool can read n number of files for the source and generate a description error code as per the requirement and provide in depth insight about the files. Below are the tasks it can perform over the incoming files from the source.

Dependency  : -
1. FileFormat.lst needs to be in place first which contains the list file formats.
2. Config file should be accurately updated as per the latest files structure.

Objective   : Performs below file checks:-

1.File size check / Zero Byte check.
2.Header validation.
3.Delimiter check.
4.Checking the column count of each record in a file and report the records along with their line number which doesn't have all the columns.
5.Checking which mandatory column of the file is having null values.
6.Removing CRTL M characters.
7.Number columns of the file with double/single quotes, wildcards characters.
