#!/usr/local/Cellar/python/3.7.6_1/bin/python3.7
##!/usr/bin/python

from __future__ import print_function
from kubernetes import client, config
from dateutil.parser import parse
from datetime import datetime, timezone, timedelta
from string import Template
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from http.server import HTTPServer, BaseHTTPRequestHandler
from signal import signal, SIGINT
import requests
import json
import time
import threading
import logging
import os
import argparse

logging.basicConfig(level=logging.INFO,
		format='%(asctime)s %(levelname)-8s %(message)s',
		datefmt='%a, %d %b %Y %H:%M:%S',
		filemode='a')
logger = logging.getLogger(__name__)

# Represents an Iter8 Experiment Custom Resource
class Experiment:
	def __init__(self, e):
		if 'metadata' in e and 'namespace' in e['metadata']:
			self.namespace = e['metadata']['namespace']
		if 'metadata' in e and 'name' in e['metadata']:
			self.name = e['metadata']['name']
		if 'status' in e and 'phase' in e['status']:
			self.phase = e['status']['phase']

		if 'spec' in e and 'targetService' in e['spec']:
			if 'baseline' in e['spec']['targetService']:
				self.baseline = e['spec']['targetService']['baseline']
			if 'candidate' in e['spec']['targetService']:
				self.candidate = e['spec']['targetService']['candidate']
			if 'name' in e['spec']['targetService']:
				self.serviceName = e['spec']['targetService']['name']
			else:
				# supporting service-based experiment no longer guarantees
				# 'targetService.name` always exists
				if 'hosts' in e['spec']['targetService']:
					self.serviceName = e['spec']['targetService']['hosts'][0]['name']
				else:
					# this should never happen
					logger.warning(f"Cannot identify a unique identifier for this experiment {e['spec']['targetService']}")
					self.serviceName = "unidentified"

		if 'status' in e:
			if 'conditions' in e['status']:
				for c in e['status']['conditions']:
					if 'type' in c:
						if c['type'] == 'RoutingRulesReady':
							self.startTime = c['lastTransitionTime']
						if c['type'] == 'ExperimentSucceeded':
							self.endTime = c['lastTransitionTime']

			self.isCompletedAndSuccessful = False
			if 'assessment' in e['status'] and 'conclusions' in e['status']['assessment']:
				if len(e['status']['assessment']['conclusions']) == 1 and \
					e['status']['assessment']['conclusions'][0] == 'All success criteria were  met' and \
					self.phase == 'Completed':
					# Only a Completed and Successful experiment is promoted
					self.isCompletedAndSuccessful = True

		self.queryTemplate = {}
		self.absentValue = {}
		self.candidateData = {}
		if 'metrics' in e:
			for m in e['metrics']:
				self.queryTemplate[m] = e['metrics'][m]['query_template']
				self.absentValue[m] = e['metrics'][m]['absent_value']

				# In case Prometheus doesn't return data either because i) the data is no longer retained
				# or ii) there is no such data collected, we use its absent_value defined for that metric
				try:
					self.candidateData[m] = float(self.absentValue[m])
				except:
					self.candidateData[m] = -1

	# Prints an Experiment Custom Resource
	def __str__(self):
		s = f"{self.namespace}.{self.name}(service:{self.serviceName}, " \
			f"baseline:{self.baseline}, candidate:{self.candidate}): " \
			f"{self.phase} ({self.startTime} - {self.endTime}) [{self.candidateData}]"
		return s

	# Convert a query template from an Experiment Custom Resource
	# to a Prometheus query used to query for a summary metric
	def getQueryStr(self, metric):
		start = parse(self.startTime)
		end = parse(self.endTime)
		now = datetime.now(timezone.utc)
		interval = end-start
		intervalStr = str(int(interval.total_seconds())) + 's'
		offset = now-end
		offsetStr = str(int(offset.total_seconds())) + 's'
		entityLabels = 'destination_service_namespace, destination_workload'

		kwargs = {
            "interval": intervalStr,
            "offset_str": f" offset {offsetStr}",
            "entity_labels": entityLabels,
        }
		qt = Template(self.queryTemplate[metric])
		query = qt.substitute(**kwargs)
		return query

	# We also get resource utilization data along with metric data, and
	# this function generates the prometheus query string
	def getResourceQueryStr(self, queryTemplate, podname):
		start = parse(self.startTime)
		end = parse(self.endTime)
		now = datetime.now(timezone.utc)
		interval = end-start
		intervalStr = str(int(interval.total_seconds())) + 's'
		offset = now-end
		offsetStr = str(int(offset.total_seconds())) + 's'

		kwargs = {
			"interval": intervalStr,
            "offset_str": f" offset {offsetStr}",
			"podname": f"{podname}",
			"namespace": self.namespace,
		}
		qt = Template(queryTemplate)
		query = qt.substitute(**kwargs)
		return query

	# Set summary metric data for candidate version
	# Default is -1 or if Prometheus has no data (expired)
	def setCandidateData(self, metric, data):
		self.candidateData[metric] = data

# This is the main engine that watches a K8s cluster for Iter8 Experiment 
# Custom Resources and query Prometheus for summary performance metrics
# It also provides a Prometheus scrape target endpoint
class Iter8Watcher:
	def __init__(self, args):

		# Prometheus URL that is used to gather metrics data
		self.prometheusURL = args.prometheus_url + '/api/v1/query'

		# Port used for Prometheus to scrape trend data
		self.scrapePort = args.scrape_port

		# Port used for Kubernetes health checking
		self.healthcheckPort = args.healthcheck_port

		# Number of seconds between listing Iter8 Experiment CRs in K8s cluster
		self.k8sFreq = args.k8s_freq

		# Initialize kubernetes.client.configuration either from a config file or
		# when running within a pod using a service account
		try:
			config.load_kube_config()
		except:
			config.load_incluster_config()
		self.kubeapi = client.CustomObjectsApi()

		# All experiments in the cluster
		self.experiments = dict()

	# At the start, we read all the Experiment Custom Resources in 
	# the cluster and query Prometheus for their summary metric data
	def loadExpFromCluster(self):
		logger.info("Loading data from Kubernetes cluster...")
		try:
			response = self.kubeapi.list_cluster_custom_object(
				group = 'iter8.tools',
				version = 'v1alpha1',
				plural = 'experiments')
			results = json.loads(json.dumps(response, ensure_ascii=False))
			for e in results['items']:
				exp = Experiment(e)
				if exp.isCompletedAndSuccessful:
					self.experiments[exp.namespace + ':' + exp.name] = exp
					for metric in exp.queryTemplate:
						self.queryPrometheusMetrics(metric, exp)
					exp.setCandidateData('cpu', self.queryPrometheusCPU(exp.candidate, exp))
					exp.setCandidateData('mem', self.queryPrometheusMEM(exp.candidate, exp))
					exp.setCandidateData('diskreadbytes', self.queryPrometheusDiskReadBytes(exp.candidate, exp))
					exp.setCandidateData('diskwritebytes', self.queryPrometheusDiskWriteBytes(exp.candidate, exp))
					exp.setCandidateData('networkreadbytes', self.queryPrometheusNetworkReadBytes(exp.candidate, exp))
					exp.setCandidateData('networkwritebytes', self.queryPrometheusNetworkWriteBytes(exp.candidate, exp))
					logger.info(exp)
		except client.rest.ApiException as e:
			logger.error(f"Exception when calling CustomObjectApi->list_cluster_custom_object: {e}")
		except Exception as e:
			logger.error(f"Unexpected error: {e}")
			exit(1)

	# Calls Prometheus to retrieve summary metric data for an Experiment
	def queryPrometheusMetrics(self, metric, exp):
		params = {'query': exp.getQueryStr(metric)}
		try:
			response = requests.get(self.prometheusURL, params=params).json()
			if 'data' in response and 'result' in response['data']:
				for res in response['data']['result']:
					if 'metric' in res and 'value' in res:
						m = res['metric']
						v = res['value']
						if m['destination_workload'] == exp.candidate:
							# v[0] is the timestamp, v[1] is the value here
							exp.setCandidateData(metric, v[1])
			else:
				logger.warning(f"Prometheus query returned no result ({params}, {response})")
		except requests.exceptions.RequestException as e:
			logger.warning(f"Problem querying Prometheus ({self.prometheusURL}): {e}")


	# Calls Prometheus to retrieve resource utilization data
	def queryPrometheusResource(self, queryTemplate, podname, exp):
		params = {'query': exp.getResourceQueryStr(queryTemplate, podname)}
		try:
			response = requests.get(self.prometheusURL, params=params).json()
			if 'data' in response and 'result' in response['data']:
				res = response['data']['result']
				if len(res) == 1:
					v = res[0]['value']
					return v[1]
				else:
					return -1
			else:
				logger.warning(f"Prometheus query returned no result ({params}, {response})")
		except requests.exceptions.RequestException as e:
			logger.warning(f"Problem querying Prometheus ({self.prometheusURL}): {e}")

	def queryPrometheusCPU(self, podname, exp):
		queryTemplate = 'sum(rate(container_cpu_usage_seconds_total{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def queryPrometheusMEM(self, podname, exp):
		queryTemplate = 'sum(avg_over_time(container_memory_working_set_bytes{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def queryPrometheusDiskReadBytes(self, podname, exp):
		queryTemplate = 'sum(rate(container_fs_reads_bytes_total{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def queryPrometheusDiskWriteBytes(self, podname, exp):
		queryTemplate = 'sum(rate(container_fs_writes_bytes_total{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def queryPrometheusNetworkReadBytes(self, podname, exp):
		queryTemplate = 'sum(rate(container_network_receive_bytes_total{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def queryPrometheusNetworkWriteBytes(self, podname, exp):
		queryTemplate = 'sum(rate(container_network_transmit_bytes_total{pod=~"$podname.*", container!~"istio-proxy", namespace="$namespace", image=~".+"}[$interval]$offset_str))'
		return self.queryPrometheusResource(queryTemplate, podname, exp)

	def startHealthCheck(self):
		class httpHandler(BaseHTTPRequestHandler):
			def do_GET(self):
				if self.path == '/api/v1/health/health_check':
					self.send_response(200)
					self.end_headers()
					self.wfile.write(bytes(json.dumps({'status': 'OK'}), 'utf-8'))
				else:
					self.send_response(404)
					self.end_headers()
		httpd = HTTPServer(('', self.healthcheckPort), httpHandler)
		httpd.serve_forever()

	# Start a Prometheus scrape target endpoint
	def startScrapeTarget(self):
		start_http_server(self.scrapePort)
		REGISTRY.register(self)
		logger.info("Starting Prometheus scrape target...")
		while True:
			time.sleep(1)

	def collect(self):
		g = GaugeMetricFamily('iter8_trend', '', labels=['namespace', 'name', 'service_name', 'time', 'metric'])
		for exp in self.experiments:
			for metric in self.experiments[exp].candidateData:
				g.add_metric([self.experiments[exp].namespace, 
							self.experiments[exp].name,
							self.experiments[exp].serviceName,
							self.experiments[exp].endTime,
							metric],
							float(self.experiments[exp].candidateData[metric]))
		yield g

	# Monitors for new Experiments in the cluster and retrieves their
	# summary metrics data from Prometheus
	def watchExpFromCluster(self):
		logger.info("Starting to watch Kubernetes cluster...")
		while True:
			try:
				response = self.kubeapi.list_cluster_custom_object(
					group = 'iter8.tools',
					version = 'v1alpha1',
					plural = 'experiments')
				results = json.loads(json.dumps(response, ensure_ascii=False))
				for e in results['items']:
					exp = Experiment(e)
					if exp.namespace + ':' + exp.name in self.experiments:
						continue
					if exp.isCompletedAndSuccessful:
						self.experiments[exp.namespace + ':' + exp.name] = exp
						for metric in exp.queryTemplate:
							self.queryPrometheusMetrics(metric, exp)
						exp.setCandidateData('cpu', self.queryPrometheusCPU(exp.candidate, exp))
						exp.setCandidateData('mem', self.queryPrometheusMEM(exp.candidate, exp))
						exp.setCandidateData('diskreadbytes', self.queryPrometheusDiskReadBytes(exp.candidate, exp))
						exp.setCandidateData('diskwritebytes', self.queryPrometheusDiskWriteBytes(exp.candidate, exp))
						exp.setCandidateData('networkreadbytes', self.queryPrometheusNetworkReadBytes(exp.candidate, exp))
						exp.setCandidateData('networkwritebytes', self.queryPrometheusNetworkWriteBytes(exp.candidate, exp))
						logger.info(exp)
		
			except client.rest.ApiException as e:
				logger.error(f"Exception when calling CustomObjectApi->list_cluster_custom_object: {e}")
			except Exception as e:
				# In case we are having problem connecting to K8s, we just quit
				logger.error(f"Unexpected error: {e}")
				os.kill(os.getpid(), SIGINT)

			time.sleep(self.k8sFreq)

	def run(self):
		# Handles ctrl-c signal
		signal(SIGINT, sighandler)

		threads = list()
		self.loadExpFromCluster()

		t0 = threading.Thread(target=self.startHealthCheck, daemon=True, args=())
		t0.start()
		threads.append(t0)

		# Start Prometheus scrape target endpoint
		t1 = threading.Thread(target=self.startScrapeTarget, daemon=True, args=())
		t1.start()
		threads.append(t1)

		# Start monitoring Iter8 Experiment Custom Resources
		t2 = threading.Thread(target=self.watchExpFromCluster, daemon=True, args=())
		t2.start()
		threads.append(t2)

		for t in threads:
			t.join()

def sighandler(signalReceived, frame):
	logger.warning('SIGINT received')
	exit(0)

def parseArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument("--scrape-port", default=8888, type=int, help="Target port number for Prometheus scraping")
	parser.add_argument("--healthcheck-port", default=8889, type=int, help="Health checking port for K8s")
	parser.add_argument("--prometheus-url", default="http://prometheus.istio-system:9090", help="Prometheus URL to get summary metrics data")
	parser.add_argument("--k8s-freq", default=30, type=int, help="Frequency to monitor K8s cluster for Iter8 Experiment Custom Resources")
	args = parser.parse_args()
	logger.info(args)
	return args

if __name__ == '__main__':
	args = parseArgs()
	watcher = Iter8Watcher(args)
	watcher.run()
