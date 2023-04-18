/*
 * Fledge Control Delivery plugin
 *
 * Copyright (c) 2022 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <plugin_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string>
#include <logger.h>
#include <plugin_exception.h>
#include <iostream>
#include <config_category.h>
#include "rapidjson/document.h"
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>
#include <rapidjson/pointer.h>
#include <sstream>
#include <unistd.h>
#include <control.h>
#include <string_utils.h>
#include <service_record.h>

using namespace std;
using namespace rapidjson;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;

#define INSTRUMENT	1
#define INSTTHRESHOLD	5

/**
 * Construct for ControlDelivery class
 *
 * @param category	The configuration of the plugin
 */
ControlDelivery::ControlDelivery(ConfigCategory *category)
{
	// Configuration set is protected by a lock
	lock_guard<mutex> guard(m_configMutex);

	// Create default values
	m_enable = false;

	// Set configuration
	this->configure(category);
}

/**
 * The destructor for the ControlDelivery class
 */
ControlDelivery::~ControlDelivery()
{
}

/**
 * Send a notification This simply sets a configuration option
 *
 * @param notificationName 	The name of this notification
 * @param triggerReason		Why the notification is being sent
 * @param message		The message to send
 */
bool ControlDelivery::notify(const string& notificationName,
			   const string& triggerReason,
			   const string& customMessage)
{
	Logger *log = Logger::getLogger();
	log->info("Delivery plugin %s: "
				   "called with trigger reason '%s'",
				   PLUGIN_NAME,
				   triggerReason.c_str());

	// Configuration fetch is protected by a mutex
	m_configMutex.lock();

	// Check for enable and for required clients
	if (!m_enable)
	{
		// Release lock	
		m_configMutex.unlock();
		log->debug("Plugin is not enabled, delivery of control message will not occur");
		return false;
	}

	/*
	 * Parse the triggerReason docuemnt and determine of this is a
	 * trigger event or a clear event. Then set the value accordingly
	 */
	string value;
	string reason;
	Document doc;
	doc.Parse(triggerReason.c_str());
	if (!doc.HasParseError())
	{
		if (doc.HasMember("reason"))
		{
			if (doc["reason"].IsString())
			{
				reason = doc["reason"].GetString();
				if (reason.compare("triggered") == 0)
				{
					value = m_triggerValue;
				}
				else
				{
					value = m_clearValue;
				}
			}
			else
			{
				log->error("Unable to determine reason for delivery, control message will not be sent");
				m_configMutex.unlock();
				return false;
			}
		}
		else
		{
			log->error("No reason for delivery is given, control message will not be sent");
			m_configMutex.unlock();
			return false;
		}
		if (doc.HasMember("data") && doc["data"].IsObject())
		{
			dataSubstitution(value, doc["data"].GetObject());
		}
	}
	else
	{
		log->error("Unable to parse reason, control message will not be sent");
		m_configMutex.unlock();
		return false;
	}
	m_configMutex.unlock();

	// Look at the JSON document in value and construct the payload to send to
	// the dispatcher service
	string path = "/dispatch/write";
	string payload;
	Document cdoc;
	cdoc.Parse(value.c_str());
	if (cdoc.HasParseError())
	{
		log->error("Failed to parse %s value: %s", reason.c_str(), value.c_str());
		return false;
	}
	else
	{
		payload = "{ \"destination\" : \"";
		if (cdoc.HasMember("service"))
		{
			payload += "service\", \"name\" : \"";
			payload += cdoc["service"].GetString();
			payload += "\", ";
		}
		else if (cdoc.HasMember("asset"))
		{
			payload += "asset\", \"name\" : \"";
			payload += cdoc["asset"].GetString();
			payload += "\", ";
		}
		else if (cdoc.HasMember("script"))
		{
			payload += "script\", \"name\" : \"";
			payload += cdoc["script"].GetString();
			payload += "\",";
		}
		else
		{
			payload += "broadcast\", ";
		}
		if (cdoc.HasMember("write"))
		{
			Value& w = cdoc["write"];
			payload += "\"write\" : { ";
			bool first = true;
			for (Value::ConstMemberIterator itr = w.MemberBegin();
					itr != w.MemberEnd(); ++itr)
			{
				if (first)
					first = false;
				else
					payload += ",";
				payload += "\"";
				payload += itr->name.GetString();
				payload += "\" : \"";
				if (itr->value.IsString())
					payload += itr->value.GetString();
				payload += "\"";

			}
			payload += "} }";
		}
		else if (cdoc.HasMember("operation"))
		{
			Value& op = cdoc["operation"];
			payload += "\"operation\" : { ";
			payload += "\"";
			payload += op["name"].GetString();
			payload += "\" : { ";
			bool first = true;
			for (Value::ConstMemberIterator itr = op.MemberBegin();
					itr != op.MemberEnd(); ++itr)
			{
				if (first)
					first = false;
				else
					payload += ",";
				payload += "\"";
				payload += itr->name.GetString();
				payload += "\" : \"";
				if (itr->value.IsString())
					payload += itr->value.GetString();
				payload += "\"";

			}
			payload += "} }";
		}
		else
		{
			log->warn("No 'operation' or 'write' in notification control request for reason %s", reason.c_str());
			log->warn("Request is: %s", value.c_str());
			return false;
		}
	}
	log->debug("Send to dispatcher %s, %s", path.c_str(), payload.c_str());
	return m_service->sendToDispatcher(path, payload);
}

/**
 * Reconfigure the delivery plugin
 *
 * @param newConfig	The new configuration
 */
void ControlDelivery::reconfigure(const string& newConfig)
{
	ConfigCategory category("new", newConfig);

	// Configuration change is protected by a lock
	lock_guard<mutex> guard(m_configMutex);

	// Set the new configuration
	this->configure(&category);
}

/**
 * Configure the delivery plugin
 *
 * @param category	The plugin configuration
 */
void ControlDelivery::configure(ConfigCategory *category)
{
	// Get the configuration category we are changing
	if (category->itemExists("service"))
	{
		m_southService = category->getValue("service");
	}

	// Get value to set on triggering
	if (category->itemExists("triggerValue"))
	{
		m_triggerValue = category->getValue("triggerValue");
	}

	// Get value to set on clearing
	if (category->itemExists("clearValue"))
	{
		m_clearValue = category->getValue("clearValue");
	}

	if (category->itemExists("enable"))
	{
       		m_enable = category->getValue("enable").compare("true") == 0 ||
			   category->getValue("enable").compare("True") == 0;
	}
}

/**
 * Substitute varaibles with reading data
 */
void ControlDelivery::dataSubstitution(string& message, const Value& obj)
{
	string rval("");
	size_t p1 = 0;

	size_t dstart;
	while ((dstart = message.find_first_of("$", p1)) != string::npos)
	{
		rval.append(message.substr(p1, dstart - p1));
		dstart++;
		size_t dend = message.find_first_of ("$", dstart);
		if (dend != string::npos)
		{
			string var = message.substr(dstart, dend - dstart);
			size_t p2 = var.find_first_of(".");
			string asset = var.substr(0, p2);
			string datapoint = var.substr(p2 + 1);
			Logger::getLogger()->debug("Looking for asset %s, data point %s",
					asset.c_str(), datapoint.c_str());
			if (obj.HasMember(asset.c_str()) && obj[asset.c_str()].IsObject())
			{
				const Value& dp = obj[asset.c_str()];
				if (dp.HasMember(datapoint.c_str()))
				{
					const Value& dpv = dp[datapoint.c_str()];
					if (dpv.IsString())
					{
						rval.append(dpv.GetString());
					}
					else if (dpv.IsDouble())
					{
						char buf[40];
						snprintf(buf, sizeof(buf), "%f", dpv.GetDouble());
						rval.append(buf);
					}
					else if (dpv.IsInt64())
					{
						char buf[40];
						snprintf(buf, sizeof(buf), "%ld", dpv.GetInt64());
						rval.append(buf);
					}
				}
				else
				{
					Logger::getLogger()->error("There is no datapoint '%s' in the '%s asset received",
						       datapoint.c_str(), asset.c_str());
				}
			}
			else
			{
				Logger::getLogger()->error("There is no asset '%s' in the data received", asset.c_str());
			}
		}
		else
		{
			Logger::getLogger()->error("Unterminated macro substitution in '%s':%ld", message.c_str(), p1);
		}
		p1 = dend + 1;
	}
	rval.append(message.substr(p1));

	Logger::getLogger()->debug("'%s'", message.c_str());
	Logger::getLogger()->debug("became '%s'", rval.c_str());
	message = rval;
}

