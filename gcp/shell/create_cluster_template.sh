#!/bin/bash
#
# @author Paolo Bargigli
# @date 2022/04/25
# 
# This script is responsible for:
#  - trigger the creation of a GCP Dataproc cluster.
#  - shh access to the Spark master node
#
# !!! Note: this script must be executed using bash 
# !!! cause sh does not support array definition
# !!! chmod +x ./create_cluster_template.sh
# !!! ./create_cluster_template.sh *args
#
#

# DEFAULT VARIABLES
# --------------------------------------------------------------
#CLUSTER_NAME_="spark-cluster-test"

# --------------------------------------------------------------
ENV="<env>"
USER="<user>"
COMPANY="<company>"
PURPOSE="<purpose>"
STACK="spark"
APPLICATION="<application>"

LABEL_KEEP_ALIVE="false"
	
# --------------------------------------------------------------
MACHINE_TYPE="n2-standard-2"
NUM_WORKERS="2"
NUM_SECONDARY_WORKERS="0"
DISK_SIZE_M="100"
DISK_SIZE_W="100"

# --------------------------------------------------------------
GCP_ZONE="<gcp_zone>"
GCP_REGION="<gcp_region>"
BUCKET="<gcp_bucket>"
GCP_PRJ="<gcp_project>"
GCP_SERV_ACC="<service_account>.iam.gserviceaccount.com"
GCP_SUBNET="projects/<project>/regions/<region>/subnetworks/<subnetwork>"
# --------------------------------------------------------------
# PYTHON_IMAGE -> function

# --------------------------------------------------------------
INIT_ACTION="gs://path/to/init_actions.sh"
JARS="gs://path/to/jars/*"
TIMEOUT="12h"
AGE="12h"
# --------------------------------------------------------------


help() {
	echo "Usage: create Dataproc cluster 
				  -n  | --cluster-name 
				[ -m  | --machine-type ]
				[ -w  | --num_workers ]
				[ -s  | --num-secondary-workers ]
				[ -dm | --disk-size-master ]
				[ -dw |--disk-size-worker ]
				[ -z  | --zone ]
				[ -r  | --region ]
				[ -b  | --bucket ]
				[ -p  | --project ]
				[ -c  | --service-account ]
				[ -i  | --python-image ]
				[ -a  | --init-action ]
				[ -j  | --jars ]
				[ -t  | --timeout ]
				[ --age ]
				[ --keep-alive ]
				[ --labe-env ]
				  --labe-user 
				  --labe-company 
				[ --labe-purpose ]
				[ --labe-stack ]
				[ --label-application ]
				"
	exit 2
}

set_var() {
	if [ -z "${2}" ]; then
		v=$(eval echo \$${1})
		export ${1}=$v
		echo "${1} unset -> using default value: $v"
	else
		v=${2}
		export ${1}=${v}
	
	echo "${1}	: ${v}"
	fi
}

get_python_image() {
	# gcloud compute images list | grep itcapy
	PYTHON_IMAGE_=$(gcloud compute images list --filter="name~"itcapy3-neuron-dp-img-*"" --sort-by NAME | tail -n 1 | awk "{print $1}")
	export PYTHON_IMAGE="projects/<project>/global/images/"${PYTHON_IMAGE_}
}
get_python_image

# echo "INPUT PARAMETERS: "${0} ${*}
# echo ""

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
	case "$1" in
		# ---------------------------------------
		-n | --cluster-name )
		set_var CLUSTER_NAME_ "${2}"
		shift 2
		#shift # past argument
		#shift # past value
		;;
		# ---------------------------------------
		-m | --machine-type )
		set_var MACHINE_TYPE "${2}"
		shift 2
		;;
		# ---------------------------------------
		-w | --num-workers )
		set_var NUM_WORKERS "${2}"
		shift 2
		;;
		# ---------------------------------------
		-s | --num-secondary-workers )
		set_var NUM_SECONDARY_WORKERS "${2}"
		shift 2
		;;
		# ---------------------------------------
		-dm | --disk-size-master )
		set_var DISK_SIZE_M "${2}"
		shift 2
		;;
		# ---------------------------------------
		-dw |--disk-size-worker )
		set_var DISK_SIZE_W "${2}"
		shift 2
		;;
		# ---------------------------------------
		-z | --zone )
		set_var GCP_ZONE "${2}"
		shift 2
		;;
		# ---------------------------------------
		-r | --region )
		set_var GCP_REGION "${2}"
		shift 2
		;;
		# ---------------------------------------
		-b | --bucket )
		set_var GCP_BUCKET "${2}"
		shift 2
		;;
		# ---------------------------------------
		-p | --project )
		set_var GCP_PRJ "${2}"
		shift 2
		;;
		# ---------------------------------------
		-a | --service-account )
		set_var GCP_SERV_ACC "${2}"
		shift 2
		;;
		# ---------------------------------------
		-c | --python-image )
		set_var PYTHON_IMAGE "${2}"
		shift 2
		;;
		# ---------------------------------------
		-a | --init-action )
		set_var INIT_ACTION "${2}"
		shift 2
		;;
		# ---------------------------------------
		-j | --jars )
		set_var JARS "${2}"
		shift 2
		;;
		# ---------------------------------------
		-t | --timeout )
		set_var TIMEOUT "${2}"
		shift 2
		;;
		# ---------------------------------------
		--age )
		set_var AGE "${2}"
		shift 2
		;;
		# ---------------------------------------

		--keep-alive )
		set_var LABEL_KEEP_ALIVE "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-env )
		set_var ENV "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-user )
		set_var USER "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-company )
		set_var COMPANY "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-purpose )
		set_var PURPOSE "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-stack )
		set_var STACK "${2}"
		shift 2
		;;
		# ---------------------------------------
		--label-application )
		set_var APPLICATION "${2}"
		shift 2
		;;
		# ---------------------------------------
		-h | --help)
		help
		;;
		# ---------------------------------------
		--)
		shift;
		break
		;;
		# ---------------------------------------
		*)
		POSITIONAL_ARGS+=("$1") # save positional arg
		shift # past argument
		;;
	esac
  done

set -- "${POSITIONAL_ARGS[@]}"  # restore positional parameters


if [ -z "${CLUSTER_NAME_}" ]; then
	echo "Error: Variable "--cluster-name" is required"
	help
	exit 1
fi
if [ -z "${USER}" ]; then
	echo "Error: Variable "--label-user" is required"
	help
	exit 1
fi
if [ -z "${COMPANY}" ]; then
	echo "Error: Variable "--label-company" is required"
	help
	exit 1
fi


#################################################

#!/bin/sh

CLUSTER_NAME="${COMPANY}-${APLICATION}-${CLUSTER_NAME_}"

### EXECUTION
gcloud dataproc clusters create ${CLUSTER_NAME} \
--bucket ${BUCKET} \
--region ${GCP_REGION} \
--zone ${GCP_ZONE} \
--image ${PYTHON_IMAGE} \
--tags allow-internal-dataproc-dev,allow-ssh-from-management-zone,allow-ssh-from-net-to-bastion \
--subnet ${GCP_SUBNET} \
--project ${GCP_PRJ} \
--service-account ${GCP_SERV_ACC} \
--master-machine-type ${MACHINE_TYPE} \
--master-boot-disk-size ${DISK_SIZE_M} \
--num-workers ${NUM_WORKERS} \
--num-secondary-workers=${NUM_SECONDARY_WORKERS} \
--worker-machine-type ${MACHINE_TYPE} \
--worker-boot-disk-size ${DISK_SIZE_W} \
--metadata=enable-oslogin=true,block-project-ssh-keys=true \
--enable-component-gateway \
--initialization-actions ${INIT_ACTION} \
--max-idle ${TIMEOUT} \
--max-age ${AGE} \
--no-address \
--properties ^#^spark:spark.jars=${JARS} \
--optional-components=JUPYTER,ZEPPELIN \
--labels \
keep_alive=${LABEL_KEEP_ALIVE},\
label=${CLUSTER_NAME},\
label_environment=${ENV},\
label_user=${USER},\
label_company=${COMPANY},\
label_purpose=${PURPOSE},\
label_stack=${STACK},\
label_application=${APPLICATION}

if [ $? -ne 0 ]; then
echo "error in cluster creation... exit."
exit 2
fi

gcloud compute ssh ${CLUSTER_NAME}-m --internal-ip --zone ${GCP_ZONE}

exit 3