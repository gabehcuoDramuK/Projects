#!/bin/sh -x
#==================================================================================================================
# Script Name :  Script_DP.sh
# Author      :  [x185885 -- Kumar Rishav]
# Date        :  25-May-2017
#
# Dependency  : 1. FileFormat.lst needs to be in place first which contains the list file formats.
#		2. Config file should be accurately updated as per the latest files structure.
#
# Objective   : Performs below file checks:-
#             	1.File size check / Zero Byte check.
#             	2.Header validation.
#             	3.Delimiter check.
#             	4.Checking the column count of each record in a file and report the records along with their line number which doesn't have all the columns.
#             	5.Checking which mandatory column of the file is having null values.
#	            6.Removing CRTL M characters.
#	            7.Number columns of the file with double/single quotes, wildcards characters.

set -vx

work=$1
landing=$2

#Extracting username and Password according to the Environment

work_temp=`echo $work|cut -f5 -d'/'`

if [ $work_temp == 'wk01' ];
   then
   UserName=`grep UserNameWk01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Password=`grep PasswordWk01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Connection=`grep ConnectionStringWk01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   work='/work/users/sfd0011/wk01/inbox/falcon'
   echo $work
   echo $UserName
   echo $Password
   echo $Connection
elif [ $work_temp == 'is02' ];
   then
   UserName=`grep UserNameIs02= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Password=`grep PasswordIs02= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Connection=`grep ConnectionStringIs02= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   work='/work/users/sft0011/is02/inbox/falcon'
   echo $work
   echo $UserName
   echo $Password
   echo $Connection

elif [ $work_temp == 'it01' ];
   then
   UserName=`grep UserNameIt01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Password=`grep PasswordIt01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Connection=`grep ConnectionStringIt01= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   work='/work/users/sfd0011/it01/inbox/falcon'
   echo $work
   echo $UserName
   echo $Password
   echo $Connection

elif [ $work_temp == 'is14' ];
   then
   UserName=`grep UserNameIs14= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Password=`grep PasswordIs14= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Connection=`grep ConnectionStringIs14= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   work='/work/users/sft0011/is14/inbox/falcon'
   echo $work
   echo $UserName
   echo $Password
   echo $Connection

else [ $work_temp == 'pr' ];
   UserName=`grep UserNamePr= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Password=`grep PasswordPr= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   Connection=`grep ConnectionStringPr= $landing/ParamFile/DB.config | cut -d '=' -f 2`
   work='/work/users/sfp0011/pr/inbox/falcon'
   echo $work
   echo $UserName
   echo $Password
   echo $Connection

fi


for x in $work/*;
do
[ -d $x ] && echo Entering into $x

#chmod -R 777 $x

while read format
do

########### OMS PROCESSING ###########

if [ "$x" == "$work/oms" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "compass_oms_vernon" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do

#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat


#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d '=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d '=' -f 2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d '=' -f 2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-

if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f8)
g=$(echo $a|cut -d"|" -f9)
h=$(echo $a|cut -d"|" -f29)
i=$(echo $a|cut -d"|" -f33)
j=$(echo $a|cut -d"|" -f34)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" } 
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" } 
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" } 
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" } 
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" } 
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" } 
if ( $29 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" } 
if ( $33 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" } 
if ( $34 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" } 
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" } 
if ( $3 ~ /^ *$/ ){printf """'"$z"'"""":"; print NR":" """'"$d"'""" } 
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" } 
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" } 
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" } 
if ( $29 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" } 
if ( $33 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" } 
if ( $34 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi


#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_OMS' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat

done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

########### LPDS PROCESSING ###########

if [ "$x" == "$work/lpds" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "LPDS" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do

#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ] 
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F',' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F',' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-

if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"," -f2)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then
awk -F"," 'NR>=0 {if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"," 'NR>=0 {if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_LPDS' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr ',' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat

done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### GIS Processing####

if [ "$x" == "$work/gis" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "PATH-DETAILS" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do

#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-

if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f6)
h=$(echo $a|cut -d"|" -f7)
i=$(echo $a|cut -d"|" -f8)
j=$(echo $a|cut -d"|" -f10)
k=$(echo $a|cut -d"|" -f12)
l=$(echo $a|cut -d"|" -f13)
m=$(echo $a|cut -d"|" -f14)
n=$(echo $a|cut -d"|" -f15)
o=$(echo $a|cut -d"|" -f16)
p=$(echo $a|cut -d"|" -f17)
q=$(echo $a|cut -d"|" -f18)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ / ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat

#awk -F"|" 'NR>=0 {
#if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
#if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
#if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
#if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
#if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
#if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
#if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
#if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
#if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
#if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
#if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }
#if ( $15 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'""" }
#if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'""" }
#if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$p"'""" }
#if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$q"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ / ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
#awk -F"|" 'NR>=0 {
#if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
#if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
#if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
#if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
#if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
#if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
#if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
#if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
#if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
#if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
#if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }
#if ( $15 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'""" }
#if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'""" }
#if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$p"'""" }
#if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$q"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_GIS' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### NC-C Processing####

if [ "$x" == "$work/nc-c" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "IDB_ACCESS_SSC" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
				   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
		   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F',' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F',' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"," -f1)
c=$(echo $a|cut -d"," -f2)
d=$(echo $a|cut -d"," -f3)
e=$(echo $a|cut -d"," -f4)
f=$(echo $a|cut -d"," -f5)
g=$(echo $a|cut -d"," -f6)
h=$(echo $a|cut -d"," -f7)
i=$(echo $a|cut -d"," -f8)
j=$(echo $a|cut -d"," -f9)
k=$(echo $a|cut -d"," -f10)
l=$(echo $a|cut -d"," -f11)
m=$(echo $a|cut -d"," -f12)
n=$(echo $a|cut -d"," -f13)
o=$(echo $a|cut -d"," -f14)
p=$(echo $a|cut -d"," -f15)
q=$(echo $a|cut -d"," -f17)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"," 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'"""}
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'"""}
if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'"""}
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'"""}
if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'"""}
if ( $15 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$p"'"""}
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$q"'"""}}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"," 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'"""}
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'"""}
if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'"""}
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'"""}
if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'"""}
if ( $15 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$p"'"""}
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$q"'"""}}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_NCC_GPON' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr ',' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"," -f$a)
                                                          awk -F',' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### CODS falcon.migration.billingaccount Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.billingaccount" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"," -f1)
c=$(echo $a|cut -d"," -f2)
d=$(echo $a|cut -d"," -f3)
e=$(echo $a|cut -d"," -f4)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_BILLING_ACCOUNT' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat

done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### CODS falcon.migration.customer Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.customer" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f6)
g=$(echo $a|cut -d"|" -f8)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_CUSTOMER' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat

done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### CODS falcon.migration.address Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.address" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_CUSTOMER_ADDRESS' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi


###### CODS falcon.migration.tel.contact Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.tel.contact" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f7)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_TELECOM_CONTACT' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### CODS falcon.migration.hsic.email Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.hsic.email" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_EMAIL' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat



done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### CODS falcon.migration.elec.contact Processing ######

if [ "$x" == "$work/cods" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.elec.contact" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f6)
h=$(echo $a|cut -d"|" -f7)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi


#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_ELECTRONIC_CONTACT' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

###### RCS_ONT Processing ######

if [ "$x" == "$work/rcs" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "RCS_ONT" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_RCS_ONT' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi


###### FMS FILES Processing ######

if [ "$x" == "$work/fms" ]
then
cd $x

#echo "Unzipping zipped files.."

#unzip -o \*.zip

ls > $landing/filecontent.dat

if [ "$format" == "BCDRVMIG" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f7)
h=$(echo $a|cut -d"|" -f10)
i=$(echo $a|cut -d"|" -f11)
j=$(echo $a|cut -d"|" -f13)
k=$(echo $a|cut -d"|" -f16)
l=$(echo $a|cut -d"|" -f17)
m=$(echo $a|cut -d"|" -f18)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_FMS_DRV' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done
rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi

if [ "$format" == "ABDRVNGPMIG" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f2)
d=$(echo $a|cut -d"|" -f3)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f7)
h=$(echo $a|cut -d"|" -f10)
i=$(echo $a|cut -d"|" -f11)
j=$(echo $a|cut -d"|" -f13)
k=$(echo $a|cut -d"|" -f16)
l=$(echo $a|cut -d"|" -f17)
m=$(echo $a|cut -d"|" -f18)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $16 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $17 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_FMS_DRV' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat



done
rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi


if [ "$format" == "BCGPNMIG" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f18)
d=$(echo $a|cut -d"|" -f22)
e=$(echo $a|cut -d"|" -f23)
f=$(echo $a|cut -d"|" -f25)
g=$(echo $a|cut -d"|" -f28)
h=$(echo $a|cut -d"|" -f30)

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $22 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $23 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $25 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $28 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $30 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $22 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $23 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $25 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $28 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $30 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_FMS_GPON' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done
rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi


if [ "$format" == "ABGPNMIG" ]
then

grep "$format" $landing/filecontent.dat > $landing/codsfilescheck.dat

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/codsfilescheck.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d'=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d'=' -f2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d'=' -f2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d"|" -f1)
c=$(echo $a|cut -d"|" -f18)
d=$(echo $a|cut -d"|" -f22)
e=$(echo $a|cut -d"|" -f23)
f=$(echo $a|cut -d"|" -f25)
g=$(echo $a|cut -d"|" -f28)
h=$(echo $a|cut -d"|" -f30)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $22 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $23 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $25 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $28 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $30 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=0 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $18 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $22 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $23 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $25 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $28 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $30 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }}' $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_FMS_GPON' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done
rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

########### CORE PROCESSING ###########

if [ "$x" == "$work/core" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "CORE" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
  then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d '=' -f2`

    #Fetch header from file
    header=`sed -n -e '2p' $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d '=' -f 2`

    #Fetch delimiter from file
    sed -n -e '2p' $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`
 if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d '=' -f 2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-

if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(sed -n -e '2p' $x/$z)
e=$(echo $a|cut -d"|" -f4)
f=$(echo $a|cut -d"|" -f5)
g=$(echo $a|cut -d"|" -f6)
h=$(echo $a|cut -d"|" -f7)


if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then

awk -F"|" 'NR>=1 && NR<v2 && NF!=12 {

if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $29 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }

}' v2="${recordcount}" $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F"|" 'NR>=1 && NR<v2 && NF!=12 {

if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $29 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }

}' v2="${recordcount}" $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_CORE' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat 
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

########### CP PROCESSING ###########

if [ "$x" == "$work/CP" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "falcon.migration.cp.customer" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do
#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>1 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat

#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
  then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d '=' -f2`

    #Fetch header from file
    header=`head -1 $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d '=' -f 2`

    #Fetch delimiter from file
    head -1 $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`
 if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d '=' -f 2`

if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F'|' 'NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-
#No Mandatory columns are there in CP file. Hence skipping this check..

#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_CP' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
head -1 $x/$z | tr '|' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=$(head -1 $x/$z)
                                                          k=$(echo $h|cut -d"|" -f$a)
                                                          awk -F'|' 'NR>1 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done

rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat

fi
fi

########### OLT PROCESSING ###########

if [ "$x" == "$work/OLT" ]
then
cd $x
ls > $landing/filecontent.dat
if [ "$format" == "OLT_Data_Extract_for_PreProvisioning" ]
then

rm $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
rm $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

for z in $(cat $landing/filecontent.dat);

do

#Removing Ctrl M Characters
sed 's/'"$(printf '\015')"'//g' $x/$z > $x/new.dat
chmod 777 $x/new.dat
cat $x/new.dat > $x/$z
rm $x/new.dat

#Fetch record count
awk 'NR>9 {print }' $x/$z > $x/rcdcnt.dat
recordcount=`wc -l "$x/rcdcnt.dat"|awk '{print $1}'`
echo $recordcount
rm $x/rcdcnt.dat


#File size check yes/no
file_size_check=`grep "$format"_FILE_SIZE_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#header validaton yes/no
header_check=`grep "$format"_HEADER_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#delimiter check yes/no
delim_check=`grep "$format"_DELIM_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#mandatory col check yes/no
mandat_col_check=`grep "$format"_MANDAT_COL_CHECK= $landing/ParamFile/config.config | cut -d '=' -f2`

#1.File size check / Zero Byte check


 if [ $file_size_check == "Y" ]
 then
   if [ $recordcount -eq 0 ]
           then
                  if [ -e $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi

           else
                  if [ -e $landing/SrcFilesLog/FILE_VALIDATION_ERRORS_$format.log ]
                   then
                         echo "$z, File Size is greater than zero byte" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                   else
                         echo "$z, File Size is greater than zero byte" > $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
                  fi
    fi
 fi


#2.Header validation

 if [ $header_check == "Y" ]
 then
    #Fetch header from config_file
    header_config=`grep "$format"_HEADER= $landing/ParamFile/config.config | cut -d '=' -f2`

    #Fetch header from file
    header=`sed -n -e '9p' $x/$z`
    header_file=$(echo $header)

    if [ $header_config != $header_file ]
           then
                         echo "$z, Header in file is invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
                         echo "$z, Header in file is valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
 fi


#3.Delimiter check

 if [ $delim_check == "Y" ]
 then
    #Fetch delimiter from config_file
    delimiter_config=`grep "$format"_DELIMITER= $landing/ParamFile/config.config | cut -d '=' -f 2`

    #Fetch delimiter from file
    sed -n -e '9p' $x/$z > $landing/SrcFilesLog/temp.dat
    delimiter_file=`grep $delimiter_config $landing/SrcFilesLog/temp.dat`

    if ! grep $delimiter_config $landing/SrcFilesLog/temp.dat
           then
           echo "$z, delimiter used in the file doesnt match with the IA and  is Invalid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
           else
           echo "$z, delimiter used in the file matches with the IA and is Valid" >> $landing/SrcFilesLog/SourceFileLogs/FILE_VALIDATION_ERRORS_$format.log
    fi
rm $landing/SrcFilesLog/temp.dat
 fi

#4.For checking the delimiter count of each record in a file and report the records along with their line number which doesn't have all the delimiters.

if [ $recordcount -gt 0 ]
then

NoOfCol=`grep "$format"_NUMBER_OF_HEADERS= $landing/ParamFile/config.config | cut -d '=' -f 2`
lastlinenumber=`wc -l $x/$z |awk '{print $1}'`
if [ -e $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat ]
then
awk -F';' 'NR>9 && NR<v2 && NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" v2="${lastlinenumber}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
else
echo FileName:LineNumber > $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
awk -F';' 'NR>9 && NR<v2 && NF !=v1 {printf """'"$z"'"""":"; print NR }' v1="${NoOfCol}" v2="${lastlinenumber}" $x/$z >> $landing/SrcFilesLog/DelimiterIssues/"$format"_DelimiterCheck.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi

#5.For Checking which mandatory column of the file is having null values :-

if [ $recordcount -gt 0 ]
then
if [ $mandat_col_check == "Y" ]
then

a=$(head -1 $x/$z)
b=$(echo $a|cut -d";" -f1)
c=$(echo $a|cut -d";" -f2)
d=$(echo $a|cut -d";" -f3)
e=$(echo $a|cut -d";" -f4)
f=$(echo $a|cut -d";" -f5)
g=$(echo $a|cut -d";" -f6)
h=$(echo $a|cut -d";" -f7)
i=$(echo $a|cut -d";" -f8)
j=$(echo $a|cut -d";" -f9)
k=$(echo $a|cut -d";" -f10)
l=$(echo $a|cut -d";" -f11)
m=$(echo $a|cut -d";" -f12)
n=$(echo $a|cut -d";" -f13)
o=$(echo $a|cut -d";" -f14)
lastlinenumber=`wc -l $x/$z |awk '{print $1}'`

if [ -e $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat ]
then
awk -F";" 'NR>9 && NR<v1 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'""" }
if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'""" }}' v1="${lastlinenumber}" $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
else
echo FileName:LineNumber:NullColumn > $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
awk -F";" 'NR>9 && NR<v1 {
if ( $1 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$b"'""" }
if ( $2 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$c"'""" }
if ( $3 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$d"'""" }
if ( $4 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$e"'""" }
if ( $5 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$f"'""" }
if ( $6 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$g"'""" }
if ( $7 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$h"'""" }
if ( $8 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$i"'""" }
if ( $9 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$j"'""" }
if ( $10 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$k"'""" }
if ( $11 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$l"'""" }
if ( $12 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$m"'""" }
if ( $13 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$n"'""" }
if ( $14 ~ /^ *$/ ) {printf """'"$z"'"""":"; print NR":" """'"$o"'""" }}' v1="${lastlinenumber}" $x/$z >> $landing/SrcFilesLog/MandatoryColNull/"$format"_MandatColNull.dat
rm $landing/SrcFilesLog/temp.dat
fi
fi
fi


#6. Checking if any number columns of the file is coming with double/single quotes , wildcards characters (?):-


rm $landing/new.dat
echo "FileName:LineNumber:ColumnName:RowData" > $landing/new.dat

#Connecting to the OracleDatabase

sqlplus -s ${UserName}/${Password}'@'//${Connection} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
set echo off
spool $landing/sqllite.dat
Select Column_name from user_tab_columns
where TABLE_NAME='STG1_OLT_TABLE_FIFA' and
DATA_TYPE='NUMBER';
spool off
EOF

#Storing the position of all the columns of the table in a temp file
sed -n -e '9p' $x/$z | tr ';' '\012'|nl > $landing/colcnt.dat


set -a arr "\"" "'" "?"
for i in "${arr[@]}"
do

if [ $i == "\"" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
              if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
              grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done
else
if [ $i == "'" ]

then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
                          if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
                          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                        else
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
                         if ( $v1 ~ /'"$i"'/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
                         grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done

else

if [ $i == "?" ]


then

                for p in $(cat $landing/sqllite.dat)
                        do
                                a=$(grep -w $p $landing/colcnt.dat | cut -f1)
                                        if [ -z "$a" ]
                                           then
                                                echo "Variable is empty"
                                           else
                                                if [ -e $landing/new.dat ]
                                                        then
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
            if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >> $landing/new.dat
           grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat

                                                        else
                                                          h=`sed -n -e '9p' $x/$z`
                                                          k=$(echo $h|cut -d";" -f$a)
                                                          awk -F';' 'NR>9 {
          if ( $v1 ~ /[?]/ ) { printf """'"$z"'"""":";printf NR":";printf """'"$k"'"""":" ;print $v1}}' v1="${a}" $x/$z >>$landing/new.dat
          grep ""$i"*"$i"" $landing/new.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
                                                fi
                                        fi
                        done


fi
fi
fi

done
echo "FileName:LineNumber:ColumnName:RowData" > $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat
sort $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat > $landing/SrcFilesLog/NumberTypeColumn/test1.dat
uniq $landing/SrcFilesLog/NumberTypeColumn/test1.dat >> $landing/SrcFilesLog/NumberTypeColumn/"$format"_NUMBERTYPECHECK.dat


done
rm $landing/SrcFilesLog/NumberTypeColumn/test1.dat
rm $landing/SrcFilesLog/NumberTypeColumn/"$format"_NumberTypeColumn.dat
rm $landing/new.dat



fi
fi

done < $landing/Scripts/FileFormat.lst
done

rm $landing/codsfilescheck.dat
rm $landing/sqllite.dat
rm $landing/colcnt.dat
rm $landing/filecontent.dat

# ----------------------------------------------------------------------------------------------------------------------
# Author    : [x185885--Kumar Rishav]
# Tile      : Changing File Names in the .lst files, for the log and data files generated by DQ tool.
# Desc      :
#             1. Remove the existing file.lst, if present.
#             2. Update the file.lst with the current names of all the log/data files present in its respective directory.


# Remove the existing list file for Delimiter check files, if present. And updating its content with latest file names.
echo $landing

chmod 777 $landing/SrcFilesLog/Delimiter.lst

rm -r $landing/SrcFilesLog/Delimiter.lst

cd $landing/SrcFilesLog/DelimiterIssues

ls > $landing/SrcFilesLog/Delimiter.lst

chmod 777 $landing/SrcFilesLog/Delimiter.lst

# Now Accessing each file and appending the path of the landing area before the file name that is mentioned within the .dat file.
for file in $(cat $landing/SrcFilesLog/Delimiter.lst)
do
Newfilename=${landing}"/SrcFilesLog/DelimiterIssues/"${file}
sed "s|${file}|${Newfilename}|g" $landing/SrcFilesLog/Delimiter.lst > File.temp
cat File.temp > $landing/SrcFilesLog/Delimiter.lst
rm -r File.temp
done


# Remove the existing list file for Mandatory check files, if present. And updating its content with latest file names.
echo $landing

chmod 777 $landing/SrcFilesLog/MandatCol.lst

rm -r $landing/SrcFilesLog/MandatCol.lst

cd $landing/SrcFilesLog/MandatoryColNull

ls > $landing/SrcFilesLog/MandatCol.lst

chmod 777 $landing/SrcFilesLog/MandatCol.lst

# Now Accessing each file and appending the path of the landing area before the file name that is mentioned within the .dat file.
for file in $(cat $landing/SrcFilesLog/MandatCol.lst)
do
Newfilename=${landing}"/SrcFilesLog/MandatoryColNull/"${file}
sed "s|${file}|${Newfilename}|g" $landing/SrcFilesLog/MandatCol.lst > File.temp
cat File.temp > $landing/SrcFilesLog/MandatCol.lst
rm -r File.temp
done

###############

# Remove the existing list file for SourceFileLogs check files, if present. And updating its content with latest file names.
echo $landing

chmod 777 $landing/SrcFilesLog/SrcLog.lst

rm -r $landing/SrcFilesLog/SrcLog.lst

cd $landing/SrcFilesLog/SourceFileLogs

ls > $landing/SrcFilesLog/SrcLog.lst

chmod 777 $landing/SrcFilesLog/SrcLog.lst

# Now Accessing each file and appending the path of the landing area before the file name that is mentioned within the .dat file.
for file in $(cat $landing/SrcFilesLog/SrcLog.lst)
do
Newfilename=${landing}"/SrcFilesLog/SourceFileLogs/"${file}
sed "s|${file}|${Newfilename}|g" $landing/SrcFilesLog/SrcLog.lst > File.temp
cat File.temp > $landing/SrcFilesLog/SrcLog.lst
rm -r File.temp
done

# Remove the existing list file for NumberColumn check files, if present. And updating its content with latest file names.
echo $landing

chmod 777 $landing/SrcFilesLog/NumberCol.lst

rm -r $landing/SrcFilesLog/NumberCol.lst

cd $landing/SrcFilesLog/NumberTypeColumn

ls > $landing/SrcFilesLog/NumberCol.lst

chmod 777 $landing/SrcFilesLog/NumberCol.lst

# Now Accessing each file and appending the path of the landing area before the file name that is mentioned within the .dat file.
for file in $(cat $landing/SrcFilesLog/NumberCol.lst)
do
Newfilename=${landing}"/SrcFilesLog/NumberTypeColumn/"${file}
sed "s|${file}|${Newfilename}|g" $landing/SrcFilesLog/NumberCol.lst > File.temp
cat File.temp > $landing/SrcFilesLog/NumberCol.lst
rm -r File.temp
done
