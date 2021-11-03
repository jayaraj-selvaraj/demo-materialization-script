##########################################################################################
##########################################################################################
# Description
#
# Note
#      This script can be used as a replacement of materialisation where the HQl/DDL would
#	   be provided by the user and the dates would be given as YYYYMMDD in the HQL/DDL.
#	   the YYYYMMDD will be replaced with the latest successful run of the write job	
##########################################################################################


export SCRIPT=wrapper_custom_materialisation.sh
if [ ${0} == ${SCRIPT} ]
then
        FILE=`ls -d -1 ${PWD}/wrapper_custom_materialisation.sh`
else
        FILE=${0}
fi

BASE_HOME_PATH=$(dirname "$(dirname "${FILE}")")
#################################
#Things to be updated
#<DECRYPT_URL>
###################################### Utilities Script #################################################################
. $BASE_HOME_PATH/../utilities/juniper_utility_wrapper.sh
###################################### Utilities Script #################################################################


#########################Initialize Script Parameters#####################################################################
PID=$$
TS=`date +%F-%H%M%S`
PWD=$(dirname "${FILE}")
DECRYP_URL="https://<DECRYPT_URL>:8095/decryption/connection"
#########################Initialize Script Parameters#####################################################################



#########################Initialize Script Argument Parameters ###########################################################
FEEDNAME=$1
TARGET_NAME=$2
RUNID=$3
######################### KMS Decrypt CALL ###############################################################################

kms_decrypt_call()
{

projectId=$1
systemSeq=$2
credentialId=$3

if [[ "${projectId}" == "" ]] || [[ "${systemSeq}" == "" ]] || [[ "${credentialId}" == "" ]]; then
	logger_error "Incorrect Arguments provided to function!"
	exit 1
fi


req="{\"projectId\":\"${projectId}\",\"systemSeq\":\"${systemSeq}\",\"credentialId\":\"${credentialId}\"}"


res=`curl -i -X POST -H 'Content-Type:application/json' -d ${req} $DECRYP_URL`

status=`echo $res | grep -oP '(?<="status": ")\w+'`
if [[ ${status,,} == "success" ]]
then
	echo $res| grep -oP '(?<="message":")\w+'
else
	logger_error "Curl Execution to decrypt KMS Failed!"
	exit 1
fi

}

#########################Creating and giving permissions to log folder  ###################################################

#exec > ${PWD}/logs/custom_$1_$(date +%F-%H%M%S).log 2>&1

logger_info "Feed name is ${FEEDNAME}"
logger_info "Target name is ${TARGET_NAME}"
logger_info "PWD is ${PWD}"
logger_info "Time-stamp & process id is ${TS}_${PID}"
logger_info "Starting custom materialisation wrapper script"
logger_info "Base home path : $BASE_HOME_PATH"

#########################Start of Materialization Script#####################################################################

###################################### Create Folders for Feed run ##########################################################
if [ ! -d "${PWD}/tmp/" ];then
        mkdir ${PWD}/tmp
	if [ $? -ne 0 ]
	then
        logger_error "Error creating the directory ${PWD}/tmp "
        exit 1
	else
		logger_info "Successfully created the directory ${PWD}/tmp "
		chmod 775 ${PWD}/tmp
		if [ $? -ne 0 ];then
        logger_error "Changing permission of ${PWD}/tmp failed please check logs"
        exit 1
        else
        logger_info "Changing permission of ${PWD}/tmp is successfully done"
        fi
	fi
fi

if [ ! -d "${PWD}/tmp/custom_${FEEDNAME}" ];then
    mkdir ${PWD}/tmp/custom_${FEEDNAME}
	if [ $? -ne 0 ]
	then
        logger_error "Error creating the directory ${PWD}/tmp/custom_${FEEDNAME} "
        exit 1
	else
		logger_info "Successfully created the directory ${PWD}/tmp/custom_${FEEDNAME} "
		chmod 775 ${PWD}/tmp/custom_${FEEDNAME}
		if [ $? -ne 0 ];then
        logger_error "Changing permission of ${PWD}/tmp/custom_${FEEDNAME} failed please check logs"
        exit 1
        else
        logger_info "Changing permission of ${PWD}/tmp/custom_${FEEDNAME} is successfully done"
        fi
	fi
fi

rm -rf ${PWD}/tmp/custom_${FEEDNAME}/*
if [ $? -ne 0 ]
then
    logger_error "Error removing the directory ${PWD}/tmp/custom_${FEEDNAME}/ "
    exit 1
else
	logger_info "Removed the directory ${PWD}/tmp/custom_${FEEDNAME}/ "
fi
###################################### Create Folders for Feed run ##########################################################


##########################Fetching the extracted date from latest successful write job ######################################

#ext_date=`oracle_execute_query -qo "select max(extracted_date) from juniper_ext_nifi_status where feed_unique_name='${FEEDNAME}' and run_id= '${RUNID}' and job_type='W' and status='SUCCESS' order by extracted_date desc;"`

ext_date=`execute_postgres_query -c "select max(extracted_date) from juniperx.juniper_ext_nifi_status where feed_unique_name='${FEEDNAME}' and run_id= '${RUNID}' and job_type='W' and status='SUCCESS' group by extracted_date order by extracted_date desc;" "|" NA`

#kms_data=`execute_postgres_query -c "select project_sequence,system_sequence,credential_id from juniperx.juniper_ext_target_conn_master where #target_unique_name='${TARGET_NAME}'" "|" NA`

#PROJECT_SEQUENCE=`echo ${kms_data} | cut -d "|" -f 1 | sed 's/ //g'`
#SYSTEM_SEQUENCE=`echo ${kms_data} | cut -d "|" -f 2 | sed 's/ //g'`
#CREDENTIAL_ID=`echo ${kms_data} | cut -d "|" -f 3 | sed 's/ //g'`

if [ $? -ne 0 ]
then
    logger_error "Extraction date for last successful write job for the feed name : ${FEEDNAME} is not successful. Please check the log file - "
    exit 1
else
	logger_info "extracted date for last successful write job is ${ext_date} for the feed name : ${FEEDNAME}"
fi

##########################Fetching the extracted date from latest successful write job ######################################


####################Fetching the connection details from source,target and customize target ##################################

execute_postgres_query -c "select
		trim(m.country_code),
		trim(m.feed_unique_name),
		n.target_conn_sequence,
		trim(n.hdp_hdfs_path),
		trim(hs.knox_username),
		trim(hc.knox_host_name),
		hc.knox_port_no,
		trim(hs.hdfs_gateway),
		COALESCE(trim(hs.hive_srvr_url),'NA'),
		hs.project_seq,
		hs.credential_id,
		trim(n.target_unique_name),
		trim(t.cust_mat_hql_path)
		FROM juniperx.JUNIPER_EXT_FEED_MASTER m
		inner join juniperx.JUNIPER_EXT_FEED_SRC_TGT_LINK link
		on m.feed_sequence=link.feed_sequence
		inner join juniperx.JUNIPER_EXT_TARGET_CONN_MASTER n
		on link.target_sequence=n.target_conn_sequence
		inner join adminx.JUNIPER_HADOOP_SERVICE_ACCOUNTS hs
		on n.hadoop_conn_sequence=hs.seq_id 
        and n.project_sequence = hs.project_seq
		inner join adminx.JUNIPER_HADOOP_CLUSTERS hc
		on hs.cluster_seq=hc.cluster_id		
		left join juniperx.JUNIPER_EXT_CUSTOMIZE_TARGET t
		on t.feed_sequence = m.feed_sequence
		where trim(m.feed_unique_name)='${FEEDNAME}'
		and trim(n.target_unique_name)='${TARGET_NAME}'" "|" ${PWD}/tmp/custom_${FEEDNAME}/keyinfo.txt

if [ $? -ne 0 ]
then
    logger_error "Connection details fetch for the feed name : ${FEEDNAME} is not successful. Please check the log file - "
    exit 1
else
	logger_info "Connection details fetch for the feed name : ${FEEDNAME} is successful. "
fi

if [ ! -s ${PWD}/tmp/custom_${FEEDNAME}/keyinfo.txt ]
then
    logger_error "No Metadata found for the feedname - ${FEEDNAME}"
    exit 1
else
	logger_info "Metadata found for the feedname - ${FEEDNAME}"
fi

####################Fetching the connection details from source,target and customize target ##################################

for file in ${PWD}/tmp/custom_${FEEDNAME}/keyinfo.txt
do
        logger_info "Reading the records"
        while read line
        do
                logger_info "Reading summary file ${line}.."
				read country system target_conn_sequence hdp_hdfs_path knox_username knox_host_name knox_port_no hdfs_gateway hive_srvr_url project_seq credential_id target cust_mat_hql_path <<< $(echo ${line} | awk -F"|" '{print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13}')

                httppath=`echo $hdfs_gateway | cut -f1,2 -d'/'`
	
PROJECT_SEQUENCE=`echo $project_seq | cut -f1,2 -d'/'`
SYSTEM_SEQUENCE="-1029"
CREDENTIAL_ID=`echo $credential_id | cut -f1,2 -d'/'`	
#########################Decrypting the password from the generic decryption jar ########################################
                #java -cp ${DECRYPTION_JAR_PATH}/decryptionJar-0.0.1-SNAPSHOT.jar com.infy.gcp.decryption.Decrypt JUNIPER_EXT_TARGET_CONN_MASTER TARGET_CONN_SEQUENCE ${target_conn_sequence} HDP_ENCRYPTED_PASSWORD ENCRYPTED_KEY ${MASTER_KEY_PATH}/master_key.txt ${PWD}/tmp/custom_${FEEDNAME}/  >/dev/null 2>&1
		hdp_password=`kms_decrypt_call ${PROJECT_SEQUENCE} ${SYSTEM_SEQUENCE} ${CREDENTIAL_ID}`
		echo $hdp_password > ${PWD}/tmp/custom_${FEEDNAME}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt

				if [ $? -ne 0 ]
				then
        			logger_error "Password decryption failed"
        			exit 1
				else
					logger_info "Password decryption successful"
				fi
#########################Decrypting the password from the generic decryption jar ########################################

#                hdp_password=`cat ${PWD}/tmp/custom_${FEEDNAME}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt | awk  '{print $1}'`

                logger_info "Running materilisation script"
				logger_info "HQL PATH : ${cust_mat_hql_path}"
				HQL_PATH=`echo ${cust_mat_hql_path}`
				hql_file_name=`basename ${HQL_PATH}`
				logger_info "HQL file name : ${hql_file_name}"

		if [ ! -f ${HQL_PATH} ]
		then
			logger_info "HQL File not found , Please check - ${HQL_PATH}"
			exit 1
		fi

#########################Replacing YYYYMMDD in the generic HQL/DDL with the the latest extracted date ########################################
		cust_ext_date=`echo $ext_date | sed 's/.\{4\}/&-/' |sed 's/.\{7\}/&-/'`

		cat $HQL_PATH | sed "s/<<YYYYMMDD>>/${ext_date}/g" |sed "s/<<yyyy-MM-dd>>/${cust_ext_date}/g" > ${PWD}/tmp/custom_${FEEDNAME}/${FEEDNAME}_${ext_date}_${hql_file_name}
	
		if [ $? -ne 0 ];then
                logger_error "Replacing YYYYMMDD with ${ext_date} failed please check logs"
                exit 1
                else
                logger_info "Replacing YYYYMMDD with ${ext_date} is successfully done"
                fi
###################################Permissions to the customized HQL/DDL created  #####################################################				
		chmod 777 ${PWD}/tmp/custom_${FEEDNAME}/${FEEDNAME}_${ext_date}_${hql_file_name}
		if [ $? -ne 0 ];then
                logger_error "Changing permission of ${PWD}/tmp/custom_${FEEDNAME}/${FEEDNAME}_${ext_date}_${hql_file_name} failed please check logs"
                exit 1
                else
                logger_info "Changing permission of ${PWD}/tmp/custom_${FEEDNAME}/${FEEDNAME}_${ext_date}_${hql_file_name} is successfully done"
                fi
###################################Permissions to the customized HQL/DDL created  #####################################################					
                ################################## Invoke Materialisation Processing ############

		beeline -u "jdbc:hive2://${hive_gateway}:${hdp_knox_port}/;ssl=true;httpPath=${httppath}/hive;transportMode=http;" -n ${hdp_user} -w ${PWD}/tmp/custom_${FEEDNAME}/JUNIPER_EXT_TARGET_CONN_MASTER_${target_conn_sequence}_HDP_ENCRYPTED_PASSWORD.txt -f ${PWD}/tmp/custom_${FEEDNAME}/${FEEDNAME}_${ext_date}_${hql_file_name} --hiveconf hive.execution.engine=tez --silent=true
		
		if [ $? -ne 0 ];then
                logger_error "Materialisation script got failed please check logs"
                exit 1
                else
                logger_info "Materialisation shell script is successfully done"
                fi
				################################## Invoke Materialisation Processing ############
        done < ${file}
done

