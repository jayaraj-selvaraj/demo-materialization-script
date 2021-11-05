#!/bin/bash
################################################################################
################################################################################
# Description
#
# Note
#    o  Runs every hour and creates databases, tables & partitions on HDFS Files
################################################################################
#set -x 
export SCRIPT=wrapper_materialisation_new.sh
if [ ${0} == ${SCRIPT} ]
then
        FILE=`ls -d -1 ${PWD}/wrapper_materialisation_new.sh`
else 
        FILE=${0}
fi

BASE_HOME_PATH=$(dirname "$(dirname "${FILE}")")
###################################
#Things to be updated.
#<DECRYPT_URL>
#
###################################### Utilities Script #################################################################
. $BASE_HOME_PATH/../utilities/juniper_utility_wrapper.sh
#########################Initialize Script Parameters#####################################################################
logger_info "Starting materialisation wrapper script"
feed_start_time=`date +%H:%M`
### MAIN ###
PID=$$
TS=`date +%s`
PWD=$(dirname "${FILE}")
LOG=`basename $0`_${TS}_${PID}.txt
MAT_LOGS_DIR=/opt/juniper/scheduler/temp/materialisation
ODATE=`date +'%Y%m%d'`
COMMON_DB="juniper"
RECON_TABLE="recon_stats_data"
DECRYP_URL="https://<DECRYPT_URL>:8095/decryption/connection"
LOG_FILE="${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${LOG}" ## need to change the logger folder
TARGET_TRIGGER_DIR="${MAT_LOGS_DIR}/trigger"
egest_hdfs_path=/apps/hive/warehouse/juniper_egest


logger_info "Timestamp & process id is ${TS}_${PID}"

#########################Initialize Script Argument Parameters ###########################################################
FEEDNAME=$1
TARGET_NAME=$2
M_RUN_ID=$3

###################################### Declaring and Define Functions#######################################################

#########################Function to Add Partitions#####################################################################
add_partitions(){
partition=$1

if [ ${cust_tgt_flag} == "N" ];then

        logger_info "adding the partition $partition"

	if [ ${staging_flag} == "N" ];then
        	echo "alter table ${dbName}.${table_name} add if not exists partition (dt=${partition}) location '${target_data_dir}/jnpr_${system}_${country}_prod.db/${table_name}/dt=${partition}';" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/addPartitions_ddl_$$.hql
	else
                echo "alter table ${dbName}.${table_name} add if not exists partition (dt=${partition}) location '${target_data_dir}/jnpr_${system}_${country}_prod_stage.db/${table_name}/dt=${partition}';" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/addPartitions_ddl_$$.hql

	fi
else

        logger_info "adding the partition $partition"
	
	if [ ${staging_flag} == "N" ];then
	        echo "alter table ${dbName}.${table_name} add if not exists partition (dt=${partition}) location '${target_data_dir}/${cust_tgt_schema}.db/${table_name,,}/dt=${partition}';" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/addPartitions_ddl_$$.hql
	else
                echo "alter table ${dbName}.${table_name} add if not exists partition (dt=${partition}) location '${target_data_dir}/${cust_tgt_schema}_stage.db/${table_name,,}/dt=${partition}';" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/addPartitions_ddl_$$.hql
	fi
fi

}

#########################Function to Create Tables#####################################################################
create_tables(){
touch ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql
touch ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tables_list_from_oracle.txt

execute_postgres_query -c "select distinct regexp_replace(table_name,'\.',' ','g') from juniperx.JUNIPER_EXT_TABLE_STATUS_VW where trim(feed_unique_name)='${system}' and trim(run_id)='${run_id}';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tables_list_from_oracle.txt


awk 'NF>0{print $NF}' ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tables_list_from_oracle.txt  > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tbl_list_${TS}_${PID}.txt

if [ ! -s ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tbl_list_${TS}_${PID}.txt ]
then
        logger_error "No table list found for the feedname - ${FEEDNAME}"
        exit 1
fi

for table_name in `cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tbl_list_${TS}_${PID}.txt`
do
start_time=`date +%H:%M`

if [ ${partition_flag} == "N" ];then

        if [ ${cust_tgt_flag} == "N" ];then

		if [ ${staging_flag} == "N" ];then
		
	        echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
        	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	        STORED AS INPUTFORMAT
	        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	        LOCATION
	        '${target_data_dir}/jnpr_${system}_${country}_prod_${run_id}_${file_date}.db/${table_name}'
	        TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${table_name}/${table_name}.avsc')
	        ;
        	" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql

		else
		
                echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                STORED AS INPUTFORMAT
                'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                LOCATION
                '${target_data_dir}/jnpr_${system}_${country}_prod_${run_id}_${file_date}_stage.db/${table_name}'
                TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${table_name}/${table_name}.avsc')
                ;
                " >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql
		
		fi

        else
		if [ ${staging_flag} == "N" ];then
		
	        echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
        	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	        STORED AS INPUTFORMAT
        	'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        	LOCATION
	        '${target_data_dir}/${cust_tgt_schema}_${file_date}.db/${table_name,,}'
        	TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${cust_tgt_schema}_${file_date}.db/${table_name}.avsc')
	        ;
        	" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql

		else

                echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                STORED AS INPUTFORMAT
                'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                LOCATION
                '${target_data_dir}/${cust_tgt_schema}_${file_date}_stage.db/${table_name,,}'
                TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${cust_tgt_schema}_${file_date}_stage.db/${table_name}.avsc')
                ;
                " >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql
		
		fi
        fi

elif [ ${partition_flag} == "Y" ];then

        if [ ${cust_tgt_flag} == "N" ];then
		
		if [ ${staging_flag} == "N" ];then
		
	        echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
	        PARTITIONED BY (dt string)
	        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	        STORED AS INPUTFORMAT
	        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	        LOCATION
	        '${target_data_dir}/jnpr_${system}_${country}_prod.db/${table_name}'
	        TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/jnpr_${system}_${country}_prod.db/${table_name}/dt=${file_date}/${table_name}.avsc');
	        ALTER TABLE ${dbName}.${table_name} SET TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/jnpr_${system}_${country}_prod.db/${table_name}/dt=${file_date}/${table_name}.avsc');
        	" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql
		
		else

                echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
                PARTITIONED BY (dt string)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                STORED AS INPUTFORMAT
                'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                LOCATION
                '${target_data_dir}/jnpr_${system}_${country}_prod_stage.db/${table_name}'
                TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/jnpr_${system}_${country}_prod_stage.db/${table_name}/dt=${file_date}/${table_name}.avsc');
                ALTER TABLE ${dbName}.${table_name} SET TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/jnpr_${system}_${country}_prod_stage.db/${table_name}/dt=${file_date}/${table_name}.avsc');
                " >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql		

		fi

        else
		if [ ${staging_flag} == "N" ];then
		
	        echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
	        PARTITIONED BY (dt string)
	        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	        STORED AS INPUTFORMAT
	        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	        LOCATION
	        '${target_data_dir}/${cust_tgt_schema}.db/${table_name,,}'
	        TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${cust_tgt_schema}.db/${table_name}.avsc')
	        ;
	        " >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql

		else

	        echo "CREATE EXTERNAL TABLE IF NOT EXISTS ${dbName}.${table_name}
        	PARTITIONED BY (dt string)
	        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	        STORED AS INPUTFORMAT
	        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	        LOCATION
	        '${target_data_dir}/${cust_tgt_schema}_stage.db/${table_name,,}'
	        TBLPROPERTIES ('avro.schema.url'='${target_metadata_dir}/${cust_tgt_schema}_stage.db/${table_name}.avsc')
	        ;
        	" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql

		fi

        fi

logger_info "Adding partitions in the created table $table_name"

part=${file_date}

                        add_partitions ${part};

else

logger_error "Invalid flag"

fi

done


if [ ${partition_flag} == "N" ];then

logger_info "Executing the HQL to create non-partitioned tables"

beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez  --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409 --silent=true

                        if [ $? -ne 0 ]
                        then
                                logger_error "create table ${table_name} is failed please check the data and metadata data-types from on hdfs location for feed name ${FEEDNAME}. Please check the log- "
                                exit 1
                        fi

elif [ ${partition_flag} == "Y" ];then

logger_info "Executing the HQL to create non-partitioned tables"

beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_createTable_ddl_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez  --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409 --silent=true

create_DDL_stat=$?

logger_info "Create_DDL_stat is ${create_DDL_stat}"

                if [[ ${create_DDL_stat} -ne 0 ]];then
                        logger_error "unable to create DDL for ${table_name} at location ${table_location_hdfs} using DDL file present at /root/tmp/${table_name}_createTable_ddl_$$.hql. Exiting.."

                        exit 1
                else
                        logger_info "created the tables"

                fi

logger_info "Executing the HQL to add all the partitions.."

beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/addPartitions_ddl_$$.hql --hiveconf hive.execution.engine=tez  --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409 --silent=true

                        if [ $? -ne 0 ]
                        then
                                logger_error "alter table for table ${table_name} for partitions is failed for feed name ${FEEDNAME}. Please check the log- "
                                exit 1
                        fi

else
logger_error "Invalid flag"

fi

if [ ${staging_flag} == "Y" ];then

	##Call Avro DB to SNAPPY DB conversion process
         logger_info "Running AVRO to SNAPPY DB conversion script.."
	 SOURCE_STG_DB=${dbName}
         TARGET_FINAL_DB=${final_dbName}
	 TARGET_PATH=${final_dbPath}

         conversion_script_path=${MAT_LOGS_DIR}/avro_to_snappy_wo_part.sh
         . ${conversion_script_path} ${SOURCE_STG_DB} ${TARGET_FINAL_DB} ${TARGET_PATH}

         if [ $? -ne 0 ];then
                logger_error "AVRO to SNAPPY Conversion script got failed.. ${conversion_script_path} ${SOURCE_STG_DB} ${TARGET_FINAL_DB} ${TARGET_PATH}  please check logs"
                exit 1
         else
                logger_info "AVRO to SNAPPY Conversion script is successfully completed ${conversion_script_path} ${SOURCE_STG_DB} ${TARGET_FINAL_DB} ${TARGET_PATH}"
         fi

fi

end_time=`date +%H:%M`

rm -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
touch ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
counter=0

chunk=1

for table_name in `cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tbl_list_${TS}_${PID}.txt`
do
 logger_info "Created the table ${dbName}.${table_name}"
 counter=$((counter+1))
 batch=$((counter/${chunk}))
 logger_info "Reconciliation: Taking counts for table ${dbName}.${table_name} in batch ${batch}"

        if [ ${partition_flag} == "N" ];then

		if [ ${staging_flag} == "N" ];then
			echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${dbName}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${dbName}.${table_name}" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
		else
			echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${TARGET_FINAL_DB}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${TARGET_FINAL_DB}.${table_name}" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
		fi

        elif [ ${partition_flag} == "Y" ];then
		
		if [ ${staging_flag} == "N" ];then

		if [ ${cust_tgt_flag} == "N" ];then
                                part=${file_date}
                                echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${dbName}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${dbName}.${table_name} where dt=${part} and INPUT__FILE__NAME like '%${M_RUN_ID}%'" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
                        else
                                part=${file_date}
                                echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${dbName}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${dbName}.${table_name} where dt=${part}" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
                        fi

                else
                        if [ ${cust_tgt_flag} == "N" ];then
                                part=${file_date}
                                echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${TARGET_FINAL_DB}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${TARGET_FINAL_DB}.${table_name} where dt=${part} and INPUT__FILE__NAME like '%${M_RUN_ID}%'" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
                        else
                                part=${file_date}
                                echo "select '${start_time}','${country}','${system}','${file_date}','${run_id}','${TARGET_FINAL_DB}','${table_name}','${end_time}','${TARGET_NAME}',count(*) from ${TARGET_FINAL_DB}.${table_name} where dt=${part}" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
                        fi
	
		fi
        else
                logger_error "Invalid flag"
        fi


        if [ `expr $counter % ${chunk}` -eq 0 ];then
                logger_info "Reconciliation: Closing batch ${batch}"
                echo ";" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql
        else
                echo "union all" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql

        fi
done
        beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt --silent=true --showHeader=false --outputformat=csv2 -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_recon_query_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409 >> ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_reconciliation_stats_${TS}_${PID}.txt
cat ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_reconciliation_stats_${TS}_${PID}.txt
}

#########################Function to process file#####################################################################
process_file()
{


                if [ ${partition_flag} == "N" ];then

                        if [ ${cust_tgt_flag} == "N" ];then
				
				if [ ${staging_flag} == "N" ];then				

                                	databaseName=${COMMON_DB}_${system}_${country}_prod_${run_id}_${file_date}
	                                dbName=`echo $databaseName | sed 's/[-]//g'`
	                                echo "create database if not exists ${dbName} location '${target_data_dir}/jnpr_${system}_${country}_prod_${run_id}_${file_date}.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql

				else
					databaseName=${COMMON_DB}_${system}_${country}_prod_${run_id}_${file_date}_stage
                                        final_databaseName=${COMMON_DB}_${system}_${country}_prod_${run_id}_${file_date}
					dbName=`echo $databaseName | sed 's/[-]//g'`
					final_dbName=`echo ${final_databaseName} | sed 's/[-]//g'`
					final_dbPath=${target_data_dir}/jnpr_${system}_${country}_prod_${run_id}_${file_date}.db
        	                        echo "create database if not exists ${dbName} location '${target_data_dir}/jnpr_${system}_${country}_prod_${run_id}_${file_date}_stage.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql
				fi
                                
				logger_info "creating database ${dbName}.."

                        else
				if [ ${staging_flag} == "N" ];then
                                	#databaseName=${COMMON_DB}_${cust_tgt_schema}_${file_date}
        	                        databaseName=${cust_tgt_schema}_${file_date}  #PROD
					dbName=`echo $databaseName | sed 's/[-]//g'`
                                	echo "create database if not exists ${dbName} location '${target_data_dir}/${cust_tgt_schema}_${file_date}.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql

				else
                                        #databaseName=juniper_${cust_tgt_schema}_${file_date}_stage #DEV
                                        #final_databaseName=juniper_${cust_tgt_schema}_${file_date} #DEV
                                        databaseName=${cust_tgt_schema}_${file_date}_stage #PROD
                                        final_databaseName=${cust_tgt_schema}_${file_date} #PROD
					dbName=`echo $databaseName | sed 's/[-]//g'`		
					final_dbName=`echo ${final_databaseName} | sed 's/[-]//g'`
					final_dbPath=${target_data_dir}/${cust_tgt_schema}_${file_date}.db
	                                echo "create database if not exists ${dbName} location '${target_data_dir}/${cust_tgt_schema}_${file_date}_stage.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql
				fi

                                logger_info "creating database ${dbName}.."
                                #echo "create database if not exists ${dbName} location '${target_data_dir}/juniper_${cust_tgt_path}_${file_date}.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql --PROD
                        fi

                        beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt --silent=true -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409

                        if [ $? -ne 0 ]
                        then
                                logger_error "create database is not able to triggered for the feed name ${FEEDNAME}. Please check the log file or beeline connection details - "
                                exit 1
                        fi

                        create_tables

                        logger_info "Performing Reconciliation for trigger file ${file} and inserting data into ${COMMON_DB}.${RECON_TABLE} table.. below are the records insererted.."
                        logger_info `cat ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_reconciliation_stats_${TS}_${PID}.txt`

        elif [ ${partition_flag} == "Y" ];then

                        if [ ${cust_tgt_flag} == "N" ];then

				if [ ${staging_flag} == "N" ];then
                                	#databaseName=jnpr_${system}_${country}_prod
	                                databaseName=${COMMON_DB}_${system}_${country}_prod
        	                        dbName=`echo $databaseName | sed 's/[-]//g'`
	                                echo "create database if not exists ${dbName} location '${target_data_dir}/jnpr_${system}_${country}_prod.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql

				else
                                        #databaseName=jnpr_${system}_${country}_prod_stage
                                        databaseName=${COMMON_DB}_${system}_${country}_prod_stage
					final_databaseName=${COMMON_DB}_${system}_${country}_prod
                                        dbName=`echo $databaseName | sed 's/[-]//g'`
  					final_dbName=`echo ${final_databaseName} | sed 's/[-]//g'`
					final_dbPath=${target_data_dir}/jnpr_${system}_${country}_prod.db
                                	echo "create database if not exists ${dbName} location '${target_data_dir}/jnpr_${system}_${country}_prod_stage.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql
				fi
                        
			        logger_info "creating database ${dbName}.."
                        
			else
				if [ ${staging_flag} == "N" ];then
                                	#databaseName=${COMMON_DB}_${cust_tgt_schema}
        	                        databaseName=${cust_tgt_schema} #PROD
					dbName=`echo $databaseName | sed 's/[-]//g'`
        	                        echo "create database if not exists ${dbName} location '${target_data_dir}/${cust_tgt_schema}.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql
				else
                                        #databaseName=juniper_${cust_tgt_schema}_stage #DEV
                                        databaseName=${cust_tgt_schema}_stage   #PROD
					dbName=`echo $databaseName | sed 's/[-]//g'`
					#final_databaseName=juniper_${cust_tgt_schema} #DEV
                                        final_databaseName=${cust_tgt_schema} #PROD
					final_dbName=`echo ${final_databaseName} | sed 's/[-]//g'`
					final_dbPath=${target_data_dir}/${cust_tgt_schema}.db
	                                echo "create database if not exists ${dbName} location '${target_data_dir}/${cust_tgt_schema}_stage.db';" > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql
				fi
                                
				logger_info "creating database ${dbName}.."
						fi

                                                echo "beeline -u 'jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;' -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409  --silent=true"

                        beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_${dbName}_createDb_ddl_${TS}_${PID}.hql --hiveconf hive.execution.engine=tez --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409  --silent=true

                        if [ $? -ne 0 ]
                        then
                                logger_error "create database is not able to triggered for the feed name ${FEEDNAME}. Please check the log file or beeline connection details - "
                                exit 1
                        fi

                        create_tables

                        logger_info "Performing Reconciliation for trigger file ${file} and inserting data into ${COMMON_DB}.${RECON_TABLE} table.. below are the records inserted.."
                        logger_info `cat ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_reconciliation_stats_${TS}_${PID}.txt`

                else
                        logger_info "Invalid db_flag"
        fi

 if [ $? -ne 0 ];then
  echo "$file" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/error_files_${TS}_${PID}
  logger_error "reconciliation failed for trigger file ${file}.. check error file ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/error_files_${TS}_${PID}"
execute_postgres_query -c "update juniperx.JUNIPER_MATERIALIZATION_MASTER set mat_status='mat failed' where trim(feed_unique_name)='${FEEDNAME}' and target_sequence in (select target_conn_sequence from juniperx.juniper_ext_target_conn_master where trim(target_unique_name)='${TARGET_NAME}') and trim(run_id)='${run_id}' and trim(mat_status)='To be Materialized';" NA NA
exit 1
 else
  logger_info "reconciliation successful for trigger file ${file}.."
 fi


}

#########################Function for Materialization#####################################################################
materilisation_processing()
{

#1. process trigger file
#------------------------#

logger_info "1. process trigger files in loop.."


logger_info "trigger file $trg_file"
        while read  trigger_files
        do
        file=`basename ${trg_file}`
        file_prefix=`basename ${file} .trg`
        logger_info "processing trigger file $file "

        read country system file_date run_id materialization_flag partition_flag cust_tgt_flag cust_tgt_path cust_tgt_schema file_format_type staging_flag <<< $(echo ${trigger_files} | awk -F "," '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11}')

                logger_info "we are processing for $country $system $file_date $run_id $materialization_flag $partition_flag $cust_tgt_flag $cust_tgt_path $cust_tgt_schema $file_format_type $staging_flag"

                if [ ${partition_flag} == "N" ];then

                        if [ ${cust_tgt_flag} == "N" ];then
                                target_data_dir="${hdp_hdfs_path}/${country}/${system}/${file_date}/${run_id}/data"
                                target_metadata_dir="${hdp_hdfs_path}/${country}/${system}/${file_date}/${run_id}/metadata"
                        else
                                target_data_dir="${hdp_hdfs_path}/${cust_tgt_path}"
                                target_metadata_dir="${hdp_hdfs_path}/${cust_tgt_path}"
                        fi

                elif [ ${partition_flag} == "Y" ];then

                        if [ ${cust_tgt_flag} == "N" ];then
                                target_data_dir="${hdp_hdfs_path}/${country}/${system}/data"
                                target_metadata_dir="${hdp_hdfs_path}/${country}/${system}/metadata"
                        else
                                target_data_dir="${hdp_hdfs_path}/${cust_tgt_path}"
                                target_metadata_dir="${hdp_hdfs_path}/${cust_tgt_path}"
                        fi
                else
                        logger_error "Invalid db_flag"
        fi


        if [ ${materialization_flag} == "Y" ];then
          process_file
        else
          logger_error "not materializing for ${file} as materialization flag is 'N'.."
          exit 1
        fi



        done < ${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${trg_file}


logger_info "processed all trigger files .."

execute_postgres_query -c "update juniperx.JUNIPER_MATERIALIZATION_MASTER set mat_status='Mat Success' where trim(feed_unique_name)='${FEEDNAME}' and target_sequence in (select target_conn_sequence from juniperx.juniper_ext_target_conn_master where trim(target_unique_name)='${TARGET_NAME}') and trim(run_id)='${run_id}' and trim(mat_status)='To be Materialized';" NA NA

logger_info "3. removing temp file after job completion.."

}

reconciliation()
{
rm ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt
file=${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_reconciliation_stats_${TS}_${PID}.txt
while read line
    do
		read start_time country system exe_date run_id db_name table_name end_time target_name tgt_count <<< $(echo ${line} | awk -F"," '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10}')
		echo $start_time $country $system $exe_date $run_id $db_name $table_name $end_time $target_name $tgt_count
 
src_count=`execute_postgres_query -c "select trim(table_count)
FROM juniperx.JUNIPER_EXT_TABLE_STATUS_VW where trim(feed_unique_name)='${system}' and trim(run_id)='${run_id}' and trim(table_name) like '%${table_name}';" NA NA`

logger_info "source count is $src_count and target count is $tgt_count"

	if [ "${src_count}" == "${tgt_count}" ];then

		logger_info "source count and target count is matching.."
		
execute_postgres_query -c "update juniperx.JUNIPER_MATERIALIZATION_MASTER set mat_status='mat done/rec matched' where trim(feed_unique_name)='${FEEDNAME}' and trim(run_id)='${M_RUN_ID}';" NA NA

	else

		logger_error "source count and target count is not matching.."
execute_postgres_query -c"update juniperx.JUNIPER_MATERIALIZATION_MASTER set mat_status='mat done/rec failed' where trim(feed_unique_name)='${FEEDNAME}' and trim(run_id)='${M_RUN_ID}';" NA NA

	fi

	done < ${file}

}

process_curls(){
echo "insert into juniperx.logger_stats_master (EVENT_FEED_ID,EVENT_BATCH_DATE,EVENT_RUN_ID,EVENT_TYPE,EVENT_VALUE,EVENT_TIMESTAMP,EVENT_IPADDRESS) values ('${system}','${file_date}','${lat_run_id}','${target_name}~${table_name}-start','${start_time}',now(),'MAT-SERVER');" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
echo "insert into juniperx.logger_stats_master (EVENT_FEED_ID,EVENT_BATCH_DATE,EVENT_RUN_ID,EVENT_TYPE,EVENT_VALUE,EVENT_TIMESTAMP,EVENT_IPADDRESS) values ('${system}','${file_date}','${lat_run_id}','${target_name}~${table_name}-end','${end_time}',now(),'MAT-SERVER');" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
echo "insert into juniperx.logger_stats_master (EVENT_FEED_ID,EVENT_BATCH_DATE,EVENT_RUN_ID,EVENT_TYPE,EVENT_VALUE,EVENT_TIMESTAMP,EVENT_IPADDRESS) values ('${system}','${file_date}','${lat_run_id}','${target_name}~${table_name}-count','${counts}',now(),'MAT-SERVER');" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
}

process_curl_feed (){
echo "insert into juniperx.logger_stats_master (EVENT_FEED_ID,EVENT_BATCH_DATE,EVENT_RUN_ID,EVENT_TYPE,EVENT_VALUE,EVENT_TIMESTAMP,EVENT_IPADDRESS) values ('${system}','${file_date}','${lat_run_id}','${target_name}~feed-start','${feed_start_time}',now(),'MAT-SERVER');" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
echo "insert into juniperx.logger_stats_master (EVENT_FEED_ID,EVENT_BATCH_DATE,EVENT_RUN_ID,EVENT_TYPE,EVENT_VALUE,EVENT_TIMESTAMP,EVENT_IPADDRESS) values ('${system}','${file_date}','${lat_run_id}','${target_name}~feed-end','${feed_end_time}',now(),'MAT-SERVER');" >> ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
}

#########################Function to add hip dashboard entries#####################################################################
hip_dashboard(){

HIP_IP=<HIP_URL>
HIP_PORT=28443

export lat_run_id=${run_id}


rm -f ${MAT_LOGS_DIR}/t/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql
touch ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql

execute_postgres_query -c "select distinct regexp_replace(table_name,'\.',' ','g') from juniperx.JUNIPER_EXT_TABLE_STATUS_VW where trim(feed_unique_name)='${system}' and trim(run_id)='${run_id}';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tables.txt

#regexp_replace(table_name,'\.',' ')

for file in ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*.txt
do
        while read line
        do
        if [ ! -z "$line" ]
        then
                logger_info "reading stat file ${line}.."
                read start_time country system exe_date run_id db_name tabl_name end_time target_name counts <<< $(echo ${line} | awk -F"," '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10}')

                                #run_id=${lat_run_id}

                logger_info `echo $start_time $country $system $exe_date $run_id $db_name $tabl_name $end_time $target_name $counts`
                                #tab_name=`echo $tabl_name| tr '[:lower:]' '[:upper:]'`  ##need to remove after test tun

                file_date=`date --date=$exe_date +"%d-%b-%y"|tr '[:lower:]' '[:upper:]'`

                read db tab scm <<< $(grep -w "${tabl_name}$" ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_tables.txt)
               
	      # if [ -z "$scm" ]
              # then
              #     table_name=${db}.${tab}		   
              # elif [ -z "$tab" ]
	      # then
              #   table_name=${db}
              # else 
	      #     table_name=${db}.${tab}.${scm}
              # fi

		if [ -z "$scm" ]
		then 
			if [ -z "$tab" ]
			then
				table_name=${db}
			else
				table_name=${db}.${tab}
			fi
		else
			table_name=${tab}.${scm}
		fi	
	   
                logger_info "HIP entry for table name ${table_name}"

                process_curls
                if [[ $? -ne 0 ]];then
                logger_error "unable to process for ${table_name}.. proceeding further.."
                                exit 1
                                else
                                logger_info "processing for feed time details"
                fi
        fi
        done < ${file}
done


for file in ${MAT_LOGS_DIR}/recon_stats_feed_st_en/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*.txt
do
        while read line
        do
                logger_info "reading stat file ${line}.."
                read feed_start_time feed_end_time system exe_date run_id <<< $(echo ${line} | awk -F"," '{print $1, $2, $3, $4, $5}')
                                #run_id=${lat_run_id}
                                logger_info `echo $feed_start_time $feed_end_time $system $exe_date $run_id`
                                file_date=`date --date=$exe_date +"%d-%b-%y"|tr '[:lower:]' '[:upper:]'`
                process_curl_feed

                if [[ $? -ne 0 ]];then
                logger_error "unable to process for ${system}.. proceeding further.."
                                else
                                logger_info "processed all files"
                fi
        done < ${file}
done

logger_info "Loading data into HIP tables"

execute_postgres_query -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql NA NA

#logger_info `cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_stats.sql | sqlplus -s @${ORA_LGN_FL}`
logger_info "publishing to HIP dashboard is successfully done"

}

drop_staging_database(){

logger_info "Dropping stage DB step started.."

NIFI_STATUS_SEQUENCE_TEMP=`execute_postgres_query -c "SELECT NIFI_STATUS_SEQUENCE from juniperx.juniper_ext_nifi_status where FEED_UNIQUE_NAME='${FEEDNAME}' and RUN_ID='${M_RUN_ID}' and JOB_TYPE='R';" NA NA`

NIFI_STATUS_SEQUENCE=`echo $NIFI_STATUS_SEQUENCE_TEMP | xargs`

NIFI_RUN_ID=$M_RUN_ID$NIFI_STATUS_SEQUENCE

execute_postgres_query -c "SELECT sum(EVENT_VALUE) as count1 from juniperx.logger_stats_master where event_feed_id='${FEEDNAME}' and event_run_id='${NIFI_RUN_ID}' and event_type like '%-count' and event_type not like '%~%-count';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_source_count.txt
        if [ $? -ne 0 ]
        then
		logger_error "Fetching for the source counts is not successful. Please check the log file"
                exit 1
        fi
		
	execute_postgres_query -c "select sum(EVENT_VALUE) as count2 from juniperx.logger_stats_master where event_feed_id='${FEEDNAME}' and event_run_id='${NIFI_RUN_ID}' and event_type like '%~%-count';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_target_count.txt

		if [ $? -ne 0 ]
        then
		
                logger_error "Fetching for the target counts is not successful. Please check the log file"
                exit 1
        fi

        if [ `cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_source_count.txt` -ne `cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_target_count.txt` ]
                then
                        logger_info "Source and target counts are not matching, not deleting the staging database."
                else
                         beeline -u "jdbc:hive2://${hive_srvr_url}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/drop_stage_database.hql --hiveconf hive.execution.engine=tez  --hiveconf  hive.tez.container.size=4096 --hiveconf hive.tez.java.opts=-Xmx3276m --hiveconf tez.runtime.io.sort.mb=1638 --hiveconf tez.runtime.unordered.output.buffer.size-mb=409 --silent=true
                        if [ $? -ne 0 ]
                                then
                                        logger_error "Dropping the staging database is not successful. Please check the log file"
                                        exit 1
                                else
                                        logger_info "Dropping the staging database is successful."
                        fi
        fi
}

date_format_update(){

   echo $cust_tgt_path | grep "<<yyyy-MM-dd>>"
   if [ $? -eq 0 ]
   then
        folder_date=`date -d "${file_date}" +'%Y-%m-%d'`
        cust_tgt_path=`echo $cust_tgt_path | sed "s/<<yyyy-MM-dd>>/${folder_date}/g"`
   fi

   echo $cust_tgt_path | grep "<<YYYYMMDD>>"
   if [ $? -eq 0 ]
   then
        folder_date=`date -d "${file_date}" +'%Y%m%d'`
        cust_tgt_path=`echo $cust_tgt_path | sed "s/<<YYYYMMDD>>/${folder_date}/g"`
   fi
        logger_info "Custom path updated : $cust_tgt_path"


}


#########################Start of Materialization Script#####################################################################
###################################### Create Folders for RUN_ID############################################################

if [ ! -d "${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}" ];then
	mkdir ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}
fi

if [ ! -d "${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}" ];then
	mkdir ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}
fi

if [ ! -d "${MAT_LOGS_DIR}/recon_stats_feed_st_en/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}" ];then
	mkdir ${MAT_LOGS_DIR}/recon_stats_feed_st_en/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}
fi

if [ ! -d "${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}" ];then
	mkdir ${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}
fi

rm -rf ${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*
rm -rf ${MAT_LOGS_DIR}/recon_stats/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*
rm -rf ${MAT_LOGS_DIR}/recon_stats_feed_st_en/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*
rm -rf ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/*

touch ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo.txt
touch ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/error_files_${TS}_${PID}


execute_postgres_query -c "select trim(m.country_code),
trim(m.feed_unique_name),
trim(m.extracted_date),
trim(m.run_id),
n.target_conn_sequence,
n.hadoop_conn_sequence,
trim(n.hdp_hdfs_path),
trim(n.materialization_flag),
trim(n.partition_flag),
coalesce(trim(t.cust_tgt_flag),'N') as cust_tgt_flag,
coalesce(trim(t.cust_tgt_path),'NA') as cust_tgt_path,
coalesce(coalesce(trim(t.cust_tgt_schema_nm),trim(t.cust_tgt_path)),'NA') as cust_tgt_schema_nm,
coalesce(trim(t.file_format_type),'AVRO') as file_format_type,
coalesce(trim(t.eod_flag),'N') as eod_flag,
n.system_sequence
FROM juniperx.JUNIPER_MATERIALIZATION_MASTER m
inner join juniperx.JUNIPER_EXT_TARGET_CONN_MASTER n
on m.target_sequence=n.target_conn_sequence 
left join juniperx.JUNIPER_EXT_CUSTOMIZE_TARGET t
on t.feed_sequence = m.feed_id
where trim(m.feed_unique_name)='${FEEDNAME}' 
and trim(m.run_id)='${M_RUN_ID}' 
and trim(n.target_unique_name)='${TARGET_NAME}';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo.txt 

if [ $? -ne 0 ]
then
        logger_info "keyinfo Connection details fetch for the feed name is not successful. Please check the log file - "
        exit 1
fi
if [ ! -s ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo.txt ]
then
        logger_error "No Metadata found for the feedname - ${FEEDNAME}"
        exit 1
fi




		logger_info "Reading the records"
        while read line
        do

                read country system file_date run_id target_conn_sequence hadoop_conn_sequence hdp_hdfs_path materialization_flag partition_flag cust_tgt_flag cust_tgt_path cust_tgt_schema file_format_type eod_flag system_sequence <<< $(echo ${line} | awk -F"\t" '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15}')

                echo "keytab details $country system $file_date $run_id $target_conn_sequence $hadoop_conn_sequence $hdp_hdfs_path $materialization_flag $partition_flag $cust_tgt_flag $cust_tgt_path $cust_tgt_schema $file_format_type $eod_flag $system_sequence"

                        execute_postgres_query -c "select
                        trim(a.knox_username),
                        trim(b.knox_host_name),
                        trim(b.knox_port_no),
                        trim(a.hdfs_gateway),
                        trim(a.hive_srvr_url),
                        a.project_seq,
                        a.credential_id
                        from adminx.JUNIPER_HADOOP_SERVICE_ACCOUNTS a
                        inner join adminx.JUNIPER_HADOOP_CLUSTERS b
                        on a.cluster_seq=b.cluster_id where a.seq_id='${hadoop_conn_sequence}';" NA ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo_conn_details.txt

                        if [ $? -ne 0 ]
                        then
                                        logger_info "Hadoop Connection details fetch for the feed name is not successful. Please check the log file - "
                                        exit 1
                        fi
                        if [ ! -s ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo_conn_details.txt ]
                        then
                                        logger_error "No Metadata found for the feedname - ${FEEDNAME}"
                                        exit 1
                        fi


                        while read srcconn
                        do
                logger_info "Read source propagation info file..."
                read hdp_user hdp_knox_host hdp_knox_port hdfs_gateway hive_srvr_url project_sequence credential_id <<< $(echo ${srcconn} | awk -F"\t" '{ print $1, $2, $3, $4, $5, $6, $7 }')

                                echo "keytab conn details are $hdp_user $hdp_knox_host $hdp_knox_port $hdfs_gateway $hive_srvr_url 4project_sequence $credential_id"

            done<${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo_conn_details.txt

                                httppath=`echo $hdfs_gateway | cut -f1,2 -d'/'`
                                trg_file=${country}_${system}_${file_date}_${run_id}_${materialization_flag}_${partition_flag}.trg
                                staging_flag="N"
#####delete

system_sequence="-1029"
                                ##Update custom target path 2 steps above to remove RegEx path TGT_FOLDER/<<DB>>_<<YYYYMMDD>>.db/<<TABLE>> - Non Partitioned DB path
                                if [[ ${cust_tgt_flag} == "Y" && ${partition_flag} == "N" ]]
                                then
                                        cust_tgt_path=`echo $cust_tgt_path | sed 's|\(.*\)/.*|\1|' | sed 's|\(.*\)/.*|\1|'`
                                        date_format_update
                                fi

                                ##Update custom target path 3 steps above to remove RegEx path TGT_FOLDER/<<DB>>/<<TABLE>>/dt=<<YYYYMMDD>> - Partitioned DB path
                                if [[ ${cust_tgt_flag} == "Y" && ${partition_flag} == "Y" ]]
                                then
                                        cust_tgt_path=`echo $cust_tgt_path | sed 's|\(.*\)/.*|\1|' | sed 's|\(.*\)/.*|\1|' | sed 's|\(.*\)/.*|\1|'`
                                        date_format_update
                                fi

                                if [ ${file_format_type} == "SNAPPY" ];then
                                        staging_flag="Y"
                                fi

                                touch ${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/$trg_file
                                logger_info "${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/$trg_file"
                                echo $country,$system,$file_date,$run_id,$materialization_flag,$partition_flag,$cust_tgt_flag,$cust_tgt_path,$cust_tgt_schema,$file_format_type,$staging_flag >> ${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/$trg_file
                                logger_info "${MAT_LOGS_DIR}/trigger/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/$trg_file"

                                JSON="{\"projectId\":\"${project_sequence}\",\"systemSeq\":\"${system_sequence}\",\"credentialId\":\"${credential_id}\"}"
                                curl -i -X POST -H 'Content-Type:application/json' -d ${JSON}  $DECRYP_URL > ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD_response.txt

                                cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD_response.txt | grep "message"|cut -d ':' -f3|cut -d '"' -f2 >${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt
                                rm -rf ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD_response.txt

                                                                
                                hdp_password=`cat ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt | grep "message"|cut -d ':' -f3|cut -d '"' -f2`


                                logger_info "Running materilisation script for the trigger files"
                                ################################## Invoke Materialisation Processing ############
                                materilisation_processing

                                if [ $? -ne 0 ];then
                                logger_error "Materialisation script got failed please check logs"
                                exit 1
                                else
                                logger_info "Materialisation shell script is successfully done"
                                fi

        done < ${MAT_LOGS_DIR}/tmp/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/keyinfo.txt



                feed_end_time=`date +%H:%M`
                echo "${feed_start_time},${feed_end_time},${system},${file_date},${run_id}" >> ${MAT_LOGS_DIR}/recon_stats_feed_st_en/${ODATE}_${FEEDNAME}_${TARGET_NAME}_${M_RUN_ID}/${file_prefix}_feed_start_end_${TS}_${PID}.txt
                mat_end_time=`date '+%d-%h-%y %H.%M.%S' | tr [:lower:] [:upper:]`




execute_postgres_query -c "update juniperx.JUNIPER_MATERIALIZATION_MASTER set mat_end_time=current_timestamp where trim(feed_unique_name)='${FEEDNAME}' and target_sequence in (select target_conn_sequence from juniperx.juniper_ext_target_conn_master where trim(target_unique_name)='${TARGET_NAME}') and trim(run_id)='${M_RUN_ID}';" NA NA




				
logger_info "Section : Running HIP dashboard script for ${FEEDNAME}"
#########################Invoke Hip Function#####################################################################
hip_dashboard
logger_info "completed HIP dashboard script for the recon stats files"


##Drop staging DB in case of AVRO to SNAPPY conversion

if [ ${staging_flag} == "Y" ];then

		drop_staging_database	
         if [ $? -ne 0 ];then
                logger_error "${drop_stage_db} Failed dropping ${SOURCE_STG_DB}"
                exit 1
         else
                logger_info "Dropping stage database ${SOURCE_STG_DB} successful. Created target DB ${TARGET_FINAL_DB}"
         fi

fi

exit 0
#########################End of Materialization script#####################################################################
