#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>
#include <deque>
#include <iterator>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <set>
#include <sys/time.h>
#include <sstream>
#include "http_parser.h"
#include "threadpool.h"
#include "rdkafkacpp.h"
using namespace std;

//==============Macroes and definition=============================
#ifdef DEBUG
#define LOG(args)  \
		log_time(); \
		cout<<"[ "<<__FUNCTION__<<" ] ";\
		cout<<args<<endl
#else
#define LOG(args)
#endif

#define LOGDIR "log"
#define LOGFILE "message_producer.log"
#define SERVER_PORT 8999
#define LISTENQ 1000
#define EPOLL_MAX 1000
#define EPOLL_EVENT_SIZE 1000
#define MAX_LINE 8192

#define FD_PAIR pair<int, int>

// Note: each thread has her own HttpInfo variable
struct HttpInfo {
	http_method method;		//Struct from http-parser
	string		url;		//Url in the http request
	int			http_fd;	//Hold the connection so we can write response
	
	//More to add,easy to extend
};

class MyEventCb : public RdKafka::EventCb {
public:
	void event_cb(RdKafka::Event &event);
};

class MyDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
	void dr_cb(RdKafka::Message &message);
};

//================Function declaration============================
void log_time();
static int on_message_begin_cb(http_parser *parser);
static int on_headers_complete_cb(http_parser *parser);
static int on_message_complete_cb(http_parser *parser);
static int on_header_field_cb(http_parser *parser, const char *at, size_t length);
static int on_header_value_cb(http_parser *parser, const char *at, size_t length);
static int on_url_cb(http_parser *parser, const char *at, size_t length);
static int on_status_cb(http_parser *parser, const char *at, size_t length);
static int on_body_cb(http_parser *parser, const char *at, size_t length);
FILE *start_log();
int end_log(FILE *fd);
int handle_http_post(string message);
int setnonblocking(int sock_fd);
void do_http_parser(void *post);
FD_PAIR start_server();
void message_loop(FD_PAIR fds);
void kafka_load_config();
void kafka_create_producer();
void kafka_load_topic_config();
void kafka_set_event_cb();
void load_threadpool_config();
void init_client_topic();
void store_client_topic();
static void do_flush(int signum);
void handle_http_get(string url);
static void statistics_qps(int signum);
void handle_post_response(void *param);

//===============Global Variables=======================
bool producer_run = true;				//Producer status
RdKafka::Producer *producer = NULL;		//Global producer
RdKafka::Conf *conf = NULL;				//Global config
RdKafka::Conf *tconf = NULL;			//Topic config
int threadpool_count = 4;				//Number of threads in threadpool
int threadpool_size = 64;				//Max tasks in the queue of threadpool task
threadpool_t *pool = NULL;				//Global threadpool
set<string> client_topic;				//All topic on the client	
pthread_key_t http_key;					//To hold thread local HttpInfo
pthread_mutex_t topic_mutex;			//Global client topic lock
pair<int, int> QPS = make_pair(0, 0);	//QPS for ELB 

int main(int argc, char *argv[]) {
	//Start Log
	FILE *log_fd = start_log();

	//Handle signals
	signal(SIGINT, do_flush);
	signal(SIGTERM, do_flush);
	signal(SIGALRM, statistics_qps);

	//Set timer for qps
	struct itimerval curr_value, old_value;
	curr_value.it_value.tv_sec = 1;
	curr_value.it_value.tv_usec = 0;
	curr_value.it_interval.tv_sec = 1;
	curr_value.it_interval.tv_usec = 0;
	setitimer(ITIMER_REAL, &curr_value, &old_value);

	//Init global mutexes, keys, and other things
	pthread_mutex_init(&topic_mutex, NULL);
	pthread_key_create(&http_key, NULL);

	//Create ThreadPool
	load_threadpool_config();
	pool = threadpool_create(threadpool_count, threadpool_size, 0);
	if (pool == NULL) {
		cerr<<"Thread pool create failed"<<endl;
		exit(1);
	}

	//Init topic stored on the client
	init_client_topic();

	//Create producer
	kafka_create_producer();

	FD_PAIR fd_pair;
	fd_pair = start_server();
	message_loop(fd_pair);

	//Do clean
	store_client_topic();
	delete producer;
	threadpool_destroy(pool, 0);
	pthread_mutex_destroy(&topic_mutex);
	pthread_key_delete(http_key);

	//End log
	end_log(log_fd);

	return 0;
}

/*	FD_PAIR.first = epoll fd
 *	FD_PAIR.second = listen fd
 */
FD_PAIR start_server() {
	struct epoll_event ev;
	int epfd = epoll_create(EPOLL_MAX);
	struct sockaddr_in servaddr;

	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = INADDR_ANY;
	servaddr.sin_port = htons(SERVER_PORT);
	
	int listenfd = socket(AF_INET, SOCK_STREAM, 0);
	if (listenfd == -1) {
		perror("Socket error");
		exit(1);
	}

	if (setnonblocking(listenfd) < 0) {
		perror("listen socket set nonblocking error");
		exit(1);
	}

	ev.data.fd = listenfd;
	ev.events = EPOLLIN | EPOLLET;
	epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);
	
	if (bind(listenfd, (sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
		perror("Socket bind error");
		exit(1);
	}

	if (listen(listenfd, LISTENQ) == -1) {
		perror("Socket listen error");
		exit(1);
	}

	return make_pair(epfd, listenfd);
}

//All HTTP POST are handled here
void message_loop(FD_PAIR fds) {
	int epfd = fds.first;
	int listenfd = fds.second;
	int nfds;
	int connectfd;
	struct sockaddr_in client_addr;
	socklen_t s_len;
	struct epoll_event ev, events[EPOLL_EVENT_SIZE];

	memset(&client_addr, 0, sizeof(client_addr));

	while (1) {
		nfds = epoll_wait(epfd, events, EPOLL_EVENT_SIZE, -1);

		for (int i = 0; i < nfds; ++i) {
			if (events[i].data.fd == listenfd) {
				while (1) {
					connectfd = accept(listenfd, (sockaddr *)&client_addr, &s_len);
					if (connectfd < 0) {
						if (errno == EAGAIN) {
							break;	//No more connections
						} else {
							perror("Socket accept error");
							exit(1);
						}
					} else {
						LOG("connect from client: "<<inet_ntoa(client_addr.sin_addr));
					}

					//Set nonblocking since it could hanging in read
					if (setnonblocking(connectfd) < 0)
						continue;	//Why the connectfd become a invalid fd?????????
					ev.data.fd = connectfd;
					//Just oneshot, this is important
					ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
					epoll_ctl(epfd, EPOLL_CTL_ADD, connectfd, &ev);
				}
			} else if (events[i].events & EPOLLIN) {
				int sockfd;
				char buffer[MAX_LINE + 1];
				string httpmessage = "";
				bool receive_success = true;

				if ((sockfd = events[i].data.fd) < 0) {
					LOG("events[i].data.fd < 0");
					continue;
				}

				//This fd is oneshot, clear it to -1
				//We are ET Trigger,so set it to -1 here is fine
				events[i].data.fd = -1;
				
				int counts;
				while (1) {
					if ((counts = read(sockfd, buffer, MAX_LINE)) < 0) {
						if (errno == ECONNRESET) {
							LOG("connection reset");
							close(sockfd);
							receive_success = false;
							break;
						} else if (errno == EAGAIN) {
							//We suppose that we have received all of the data
							break;
						}else {
							LOG("socket read error: "<<strerror(errno));
							close(sockfd);
							receive_success = false;
							break;
						}
					} else if (counts == 0) {
						LOG("connection closed by peer");
						receive_success = false;
						close(sockfd);
						break;
					}

					buffer[counts] = '\0';
					httpmessage += buffer;
				}

				//Caculate QPS since we have read from the socket
				++QPS.second;

				//If we receive request successfully
				if (receive_success) {
					//Finish read http post,now do the parser
					//Message is fd#message, fd is used to send response
					//Since we have to give different response
					//We must transfer this fd,so HttpInfo in each thread can hold it
					stringstream ss_fd;
					string httpfd;
					ss_fd<<sockfd<<"#";
					ss_fd>>httpfd;
					httpmessage.insert(0, httpfd);
					threadpool_add(pool, &do_http_parser, (void *)httpmessage.c_str(), 0);	
				}
			} else if (events[i].events & EPOLLOUT) {
				//Program now does not go hear for any reason
				//Keep this for future use
			} else {
				LOG("EPOLL goes to the last else");
			}
		}
	}
}

void do_http_parser(void *post) {
	http_parser_settings settings;

	settings.on_message_begin = on_message_begin_cb;
	settings.on_headers_complete = on_headers_complete_cb;
	settings.on_message_complete = on_message_complete_cb;
	settings.on_header_field = on_header_field_cb;
	settings.on_header_value = on_header_value_cb;
	settings.on_url = on_url_cb;
	settings.on_status = on_status_cb;
	settings.on_body = on_body_cb;

	http_parser *parser = new http_parser;
	http_parser_init(parser, HTTP_REQUEST);

	//Get connection fd from the message
	//Thread local HttpInfo hold the connection
	HttpInfo *http_info = new HttpInfo;
	string temp((char *)post);
	string httpfd = temp.substr(0, temp.find("#"));
	http_info->http_fd = atoi(httpfd.c_str());
	pthread_setspecific(http_key, http_info);
	
	//Parse the message
	string message = temp.substr(temp.find("#") + 1);
	size_t npassed = http_parser_execute(parser, &settings, message.c_str(), message.length());
}

void log_time() {
	time_t tm = time(NULL);
	char *temp = ctime(&tm);
	string now(temp, 0, strlen(temp) - 1);
	cout<<"["<<now<<"] ";
}

static int on_message_begin_cb(http_parser *parser) {
	//Do things here
	LOG("Message begin");

	return 0;
}

static int on_headers_complete_cb(http_parser *parser) {
	//Do things here
	LOG("Headers parse complete");
	
	HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);
	http_info->method = (http_method)parser->method;

	if (http_info->method == HTTP_GET) {
		handle_http_get(http_info->url);
	}
	
	return 0;
}

static int on_message_complete_cb(http_parser *parser) {
	//Do things here
	LOG("Body parse complete");

	delete parser;
	
	return 0;
}

static int on_header_field_cb(http_parser *parser, const char *at, size_t length) {
	//Do things here
	string header_field(at, 0, length);
    LOG("HEADER FIELD: "<<header_field);
	
	return 0;
}

static int on_header_value_cb(http_parser *parser, const char *at, size_t length) {
	//Do things here
	string header_value(at, 0, length);
	LOG("HEADER VALUE: "<<header_value);
	
	return 0;
}

static int on_url_cb(http_parser *parser, const char *at, size_t length) {
	//Do things here
	string url(at, 0, length);
	LOG("URL: "<< url);

	HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);
	http_info->url = url;

	return 0;
}

static int on_status_cb(http_parser *parser, const char *at, size_t length) {
	//Do things here
	string response_status(at, 0, length);
	LOG("RESPONSE STATUS: "<<response_status);
	
	return 0;
}

static int on_body_cb(http_parser *parser, const char *at, size_t length) {
	//Do things here
	string body(at, 0, length);
	LOG("BODY: "<< body);

	HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);

	//We only handle HTTP POST
	if (http_method(http_info->method) == HTTP_GET) {
		LOG("Data from HTTP GET");
		
		//We should not process HTTP GET which has body in it now
		return 0;
	} else if (http_method(http_info->method) == HTTP_POST){
		LOG("Data from HTTP POST");

		//Response when we finish parsing
		stringstream ss_fd;
		string *httpfd = new string;
		HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);
		ss_fd<<http_info->http_fd;
		ss_fd>>*httpfd;
		threadpool_add(pool, &handle_post_response, (void *)httpfd, 0);

		int ret = handle_http_post(body);
		if (ret == 1) {
			LOG("HTTP post values wrong");
		} else if (ret == 2) {
			LOG("Wrong http request");
		}
	}

	return 0;
}

FILE *start_log() {
	DIR *dir = opendir(LOGDIR);
	if (dir == NULL) 
		mkdir(LOGDIR, S_IRWXU | S_IRWXG | S_IRWXO);
	else
		closedir(dir);

	string log_file = string(LOGDIR) + "/" + LOGFILE;
	FILE *fd = freopen(log_file.c_str(), "a", stdout);
	if (fd == NULL) {
		perror("Log file open failed");
		exit(1);
	}
	return fd;
}

int end_log(FILE *fd) {
	return fclose(fd);
}

/*	Return 1 if http post values wrong
 *	Return 2 if http request is wrong
 *	Return 0 if everything if fine
 *	NOTE: type=xxx&topic=xxx&message=xxx&Submit=xxx
 *		  type, topic, message should not be the last
 *		  post parameter,this makes things easy to be
 *		  handled
 */
int handle_http_post(string message) {
	HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);
	if (http_info->url == "/producer") {
		if (message.find("controltype=") == string::npos)
			return 1;
		else if (producer_run) {
			//Get control type
			string controltype;
			size_t typestart = message.find("controltype=");
			size_t typeend = message.find("&", typestart);
			controltype = message.substr(typestart + 12, typeend - typestart - 12);

			if (controltype == "ProduceMessage") {
				size_t start, end;
				string topicstr, messagestr;
				start = message.find("topic=");
				if (start == string::npos)
					return 1;
				end = message.find("&", start);
				topicstr = message.substr(start + 6, end - start - 6);

				//Check whether we have this topic,if not we just do nothing (or give a different http response)
				if (client_topic.find(topicstr) == client_topic.end()) {
					LOG("topic: "<<topicstr<<" not exist on the client");
					return 0;	//Just return,don't do any further processging 
				}

				start = message.find("message=");
				if (start == string::npos)
					return 1;
				end = message.find("&", start);
				messagestr = message.substr(start + 8, end - start - 8);

				//Produce message
				string errstr;
				RdKafka::Topic *topic = RdKafka::Topic::create(producer, topicstr, tconf, errstr);

				if (!topic) {
					cerr<<"Failed to create topic: "<<errstr<<endl;
					exit(1);
				}

				RdKafka::ErrorCode resp = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(messagestr.c_str()), messagestr.length(), NULL, NULL);

				if (resp != RdKafka::ERR_NO_ERROR)
					cerr<<"% Produce failed: "<<RdKafka::err2str(resp)<<endl;
				else
					LOG("% Produce message ("<<messagestr.length()<<" byetes)");

				producer->poll(0);

				delete topic;
			}  else if (controltype == "CreateTopic") {
				size_t start, end;
				start = message.find("topic=");
				if (start == string::npos)
					return 1;
				end = message.find("&", start);
				string topicstr = message.substr(start + 6, end - start - 6);

				if (client_topic.find(topicstr) != client_topic.end()) {
					LOG("topic: "<<topicstr<<" already exists on the client");
					return 0;	//Topic already exist on the client,just return
				}

				string errstr;
				RdKafka::Topic *topic = RdKafka::Topic::create(producer, topicstr, tconf, errstr);

				if (!topic) {
					cerr<<"Failed to create topic: "<<errstr<<endl;
					exit(1);
				}

				// STL is not thread safe, lock it
				pthread_mutex_lock(&topic_mutex);
				client_topic.insert(topicstr);
				pthread_mutex_unlock(&topic_mutex);
			} else {
				return 1;	//Type we not supported
			}
		} else {
			cerr<<"Producer down"<<endl;
			exit(1);
		}
	} else {
		return 2;	//Wrong request
	}

	return 0;
}

int setnonblocking(int sock_fd) {
	int opts;
	opts = fcntl(sock_fd, F_GETFL);

	if (opts < 0) {
		LOG("fcntl(sock_fd, F_GETFL)"<<strerror(errno)<<" "<<sock_fd);
		return -1;
	}

	opts = opts | O_NONBLOCK;
	if (fcntl(sock_fd, F_SETFL, opts) < 0) {
		LOG("fcntl(sock_fd, F_SETFL, opts)"<<strerror(errno)<<" "<<sock_fd);
		return -1;
	}

	return 0;
}

void kafka_create_producer() {
	conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	string errstr;
	kafka_load_config();
	kafka_load_topic_config();
	kafka_set_event_cb();
	
	producer = RdKafka::Producer::create(conf, errstr);
	if (!producer) {
		cerr<<"Failed to create producer: "<<errstr<<endl;
		exit(1);
	}

	LOG("Created producer "<< producer->name());
}

void kafka_load_config() {
	FILE *config_fp = fopen("config/kafka_producer.config", "r");
	if (config_fp == NULL) {
		perror("config file not exists");
		exit(1);
	}
	
	char config_ele[100];
	string configstr, configkey, configvalue, errstr;
	while (fgets(config_ele, 100, config_fp) != NULL) {
		if (config_ele[0] == '#')
			continue;	//Pass comments
		configstr = config_ele;
		int pos = configstr.find(' ');
		configkey = configstr.substr(0, pos);
		configvalue = configstr.substr(pos + 1, configstr.length() - pos - 2);
		if (conf->set(configkey, configvalue, errstr) != RdKafka::Conf::CONF_OK) {
			cerr<<"config: "<<configkey<<" "<<configvalue<<"set error => "<<errstr<<endl;
			exit(1);
		}
	}

	fclose(config_fp);
}

void kafka_load_topic_config() {
	FILE *config_fp = fopen("config/kafka_producer_topic.config", "r");
	if (config_fp == NULL) {
		perror("topic config file not exists");
		exit(1);
	}
	
	char config_ele[100];
	string configstr, configkey, configvalue, errstr;
	while (fgets(config_ele, 100, config_fp) != NULL) {
		if (config_ele[0] == '#')
			continue;	//Pass comments
		configstr = config_ele;
		int pos = configstr.find(' ');
		configkey = configstr.substr(0, pos);
		configvalue = configstr.substr(pos + 1, configstr.length() - pos - 2);
		if (conf->set(configkey, configvalue, errstr) != RdKafka::Conf::CONF_OK) {
			cerr<<"topic config: "<<configkey<<" "<<configvalue<<"set error => "<<errstr<<endl;
			exit(1);
		}
	}

	fclose(config_fp);
}

void kafka_set_event_cb() {
	MyEventCb *my_event_cb = new MyEventCb;
	MyDeliveryReportCb *my_dr_cb = new MyDeliveryReportCb;
	string errstr;
	conf->set("event_cb", my_event_cb, errstr);
	conf->set("dr_cb", my_dr_cb, errstr);
}

void MyEventCb::event_cb(RdKafka::Event &event) {
	switch (event.type()) {
		case RdKafka::Event::EVENT_ERROR:
			LOG("ERROR ("<<RdKafka::err2str(event.err()) <<"): "<<event.str());
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				producer_run = false;
			break;
		case RdKafka::Event::EVENT_STATS:
			LOG("\"STATS\": "<<event.str());
			break;
		case RdKafka::Event::EVENT_LOG:
			LOG("LOG-"<<event.severity()<<"-"<<event.fac().c_str()<<": "<<event.str().c_str());
			break;
		default:
			LOG("EVENT "<<event.type()<<" ("<<RdKafka::err2str(event.err())<<") :"<<event.str());
			break;
	}
}

void MyDeliveryReportCb::dr_cb(RdKafka::Message &message) {
	LOG("Message delivery for ("<<message.len()<<" bytes): "<<message.errstr());
	if (message.key()) {
		LOG("Key: "<<*(message.key())<<";");
	}
}

void load_threadpool_config() {
	FILE *config_fp = fopen("config/threadpool.config", "r");
	if (config_fp == NULL) {
		perror("threadpool config file not exists");
		exit(1);
	}
	
	char config_ele[100];
	string configstr, configkey, configvalue, errstr;
	while (fgets(config_ele, 100, config_fp) != NULL) {
		if (config_ele[0] == '#')
			continue;	//Pass comments
		configstr = config_ele;
		int pos = configstr.find(' ');
		configkey = configstr.substr(0, pos);
		configvalue = configstr.substr(pos + 1, configstr.length() - pos - 2);
		if (configkey == "threadpool.thread.count") {
			threadpool_count = atoi(configvalue.c_str());
		} else if (configkey == "threadpool.thread.task.size") {
			threadpool_size = atoi(configvalue.c_str());
		} else {
			cerr<<"config: "<<configkey<<" "<<configvalue<<" is not a valid config"<<endl;
			exit(1);
		}
	}

	fclose(config_fp);
}

void init_client_topic() {
	FILE *topic_fp = fopen("data/producer.topic", "r");
	if (topic_fp == NULL) {
		perror("producer.topic file not exists");
		exit(1);
	}

	char topic[100];
	while (fgets(topic, 100, topic_fp) != NULL) {
		if (topic[0] == '#')
			continue;
		string temp(topic, 0, strlen(topic) - 1);
		client_topic.insert(temp);
	}

	fclose(topic_fp);
}

void store_client_topic() {
	FILE *topic_fp = fopen("data/producer.topic", "w");
	if (topic_fp == NULL) {
		perror("producer.topic file not exists");
		exit(1);
	}
	
	fputs("#this file saves producer client topics\n", topic_fp);

	set<string>::iterator it = client_topic.begin();
	for (; it != client_topic.end(); ++it) {
		fputs(it->c_str(), topic_fp);
		fputs("\n", topic_fp);
	}

	fclose(topic_fp);
}

static void do_flush(int signum) {
	store_client_topic();		
	exit(1);
}

void handle_http_get(string url) {
	HttpInfo *http_info = (HttpInfo *)pthread_getspecific(http_key);
	if (url == "/check") {
		//Heartbeat 
		string response = "HTTP/1.1 200 OK";
		write(http_info->http_fd, response.c_str(), response.length());
		close(http_info->http_fd);
	} else if (url == "/qps") {
		//QPS statistics
		string response;
		stringstream qps_info;
		response = string("HTTP/1.1 200 OK\n") + 
				   "Content-Length: ";
		qps_info<<QPS.first;
		string temp = qps_info.str();
		qps_info.clear();	//Clear status
		qps_info.str("");	//Clear stream
		qps_info<<temp.length()<<"\n\n";
		response += qps_info.str() + temp;
		write(http_info->http_fd, response.c_str(), response.length());
		close(http_info->http_fd);
	} else {
		LOG("Wrong HTTP GET");
	}
}

static void statistics_qps(int signum) {
	QPS.first = QPS.second - QPS.first;
	QPS.second = QPS.first;
}

/*
 * Keep param for future use
 */
void handle_post_response(void *http_fd) {
	int httpfd = atoi(((string *)http_fd)->c_str());
	string response = "HTTP/1.1 200 OK";
	write(httpfd, response.c_str(), response.length());

	LOG("Post Response");

	close(httpfd);
	delete (string *)http_fd;
}
