package main

import (
	"bufio"
	"encoding/json"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

const logStreamName = "journal"
const maxRetries = 10

var ignoreUnits = map[string]bool{
	"journald-cloudwatch.service": true,
}
var ignoreFields = map[string]bool{
	"__MONOTONIC_TIMESTAMP":      true,
	"__CURSOR":                   true,
	"__REALTIME_TIMESTAMP":       true,
	"_BOOT_ID":                   true,
	"_MACHINE_ID":                true,
	"_SOURCE_REALTIME_TIMESTAMP": true,
	"_CAP_EFFECTIVE":             true,
}

type Event *cloudwatchlogs.InputLogEvent

func main() {
	relay := &Relay{}
	relay.run()
}

type Relay struct {
	sequenceToken *string
	client        *cloudwatchlogs.CloudWatchLogs
	logGroup      string
	logStream     string
	batch         []*cloudwatchlogs.InputLogEvent
	failures      int
}

func (self *Relay) run() {
	sess := session.New()
	self.logGroup, _ = os.Hostname()
	self.logStream = logStreamName
	self.client = cloudwatchlogs.New(sess)
	self.createLogGroup()
	self.createLogStream()

	eventCh := make(chan *cloudwatchlogs.InputLogEvent, 1)
	go journalTailer(eventCh)
	timer := time.NewTimer(time.Second)
	for {
		select {
		case event := <-eventCh:
			self.batch = append(self.batch, event)
		case <-timer.C:
			if len(self.batch) > 0 {
				self.sendEvents()
				// exponential backoff
				if self.failures > maxRetries {
					log.Printf("Retries exceeded - dropping %d events", len(self.batch))
					self.batch = nil
					self.failures = 0
				}
			}
			delay := time.Second * time.Duration(math.Pow(2, float64(self.failures)))
			timer.Reset(delay)
		}
	}
}

func (self *Relay) sendEvents() {
	if self.failures > 0 {
		log.Printf("Retrying attempt: #%d", self.failures)
	}
	input := cloudwatchlogs.PutLogEventsInput{
		LogEvents:     self.batch,
		LogGroupName:  aws.String(self.logGroup),
		LogStreamName: aws.String(self.logStream),
		SequenceToken: self.sequenceToken,
	}
	output, err := self.client.PutLogEvents(&input)
	if err != nil {
		self.failures++
		log.Println("Failed to put log events:", err)
		if err, ok := err.(awserr.Error); ok {
			if err.Code() == "InvalidSequenceTokenException" {
				log.Println("Refreshing Sequence Token...")
				self.createLogStream()
			}
		}
		return
	}

	if output.RejectedLogEventsInfo != nil {
		log.Println("Rejections:", output.RejectedLogEventsInfo)
	}
	log.Printf("Sent %d log events", len(self.batch))
	self.batch = nil
	self.failures = 0
	if output.NextSequenceToken != nil {
		self.sequenceToken = output.NextSequenceToken
	}
}

var reUnderscore = regexp.MustCompile("_(.)")

func replacer(s string) string {
	return strings.ToUpper(s[1:])
}

func parseLine(line string) (KV, error) {
	var original KV
	err := json.Unmarshal([]byte(line), &original)
	return original, err
}

func transformKey(key string) string {
	key = strings.ToLower(key)
	key = strings.TrimLeft(key, "_")
	key = reUnderscore.ReplaceAllStringFunc(key, replacer)
	return key
}

type KV map[string]interface{}

func transformData(original KV) Event {
	transformed := KV{}
	for key, value := range original {
		if ignoreFields[key] {
			continue
		}
		transformed[transformKey(key)] = value
	}

	timestamp, _ := strconv.ParseInt(original["__REALTIME_TIMESTAMP"].(string), 10, 64)
	message, _ := json.Marshal(transformed)
	event := cloudwatchlogs.InputLogEvent{
		Message:   aws.String(string(message)),
		Timestamp: aws.Int64(timestamp / 1000),
	}
	return &event
}

func filterEvent(data map[string]interface{}) bool {
	if value, ok := data["_SYSTEMD_UNIT"].(string); ok {
		if ignoreUnits[value] {
			return true
		}
	}
	if value, ok := data["_SYSTEMD_USER_UNIT"].(string); ok {
		if ignoreUnits[value] {
			return true
		}
	}
	return false
}

func journalTailer(ch chan *cloudwatchlogs.InputLogEvent) {
	log.Println("Tailing journal...")
	cmd := exec.Command("journalctl", "-f", "-n0", "-q", "--output=json")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		data, err := parseLine(scanner.Text())
		if err != nil {
			log.Println("Error decoding json:", err)
			continue
		}
		if filterEvent(data) {
			continue
		}
		event := transformData(data)
		ch <- event
	}
}

func (self *Relay) createLogGroup() {
	log.Printf("Checking log group '%s'...", self.logGroup)
	input := cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(self.logGroup),
	}
	output, err := self.client.DescribeLogGroups(&input)
	if err != nil {
		log.Fatalln("DescribeLogGroups error:", err)
	}
	exists := false
	for _, group := range output.LogGroups {
		if *group.LogGroupName == self.logGroup {
			exists = true
		}
	}
	if !exists {
		log.Printf("Creating new log group '%s'...", self.logGroup)
		input := cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(self.logGroup),
		}
		_, err := self.client.CreateLogGroup(&input)
		if err != nil {
			log.Fatalln("CreateLogGroup:", err)
		}
		log.Printf("Created log group '%s' successfully", self.logGroup)
	}
}

func (self *Relay) createLogStream() {
	log.Printf("Checking log stream '%s'...", self.logStream)
	input := cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(self.logGroup),
		LogStreamNamePrefix: aws.String(self.logStream),
	}
	output, err := self.client.DescribeLogStreams(&input)
	if err != nil {
		log.Fatalln("DescribeLogStreams:", err)
	}
	exists := false
	for _, group := range output.LogStreams {
		if *group.LogStreamName == self.logStream {
			exists = true
		}
	}
	if !exists {
		input := cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(self.logGroup),
			LogStreamName: aws.String(self.logStream),
		}
		_, err := self.client.CreateLogStream(&input)
		if err != nil {
			log.Fatalln("CreateLogStream:", err)
		}
		log.Printf("Created log stream '%s' successfully", self.logStream)
		return
	}

	log.Printf("Continuing log stream '%s'...", self.logStream)
	self.sequenceToken = output.LogStreams[0].UploadSequenceToken
}
