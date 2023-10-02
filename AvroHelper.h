/*
   PROGRAM: AVRO Serializer and Deserializer
   AUTHOR:  Afshin Ahmadi
   DATE:    2023-09-09
   VER:		1.0
   HELP:    https://avro.apache.org/docs/1.11.1/api/cpp/html/
*/

#pragma once

#include <vector>

#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"
#include "avro/Serializer.hh"
#include "avro/Writer.hh"



/*
*********************** Class to encode data to binary Avro standard ***********************
*/
class AvroBinarySerializer
{
public:
	/*	
	* Default class constructor. 
	* OutputStream grows in memory chunks of ChunkSize (default value is 4096). Use a smaller size to save memory. 
	*/
	AvroBinarySerializer(size_t ChkSize=4096Ui64) {
		ChunkSize_ = ChkSize;
		out_ = avro::memoryOutputStream(ChkSize);
		e_ = avro::binaryEncoder();
		e_->init(*out_);
	}
	
	/*
	* Class constructor with Schema validation.
	* OutputStream grows in memory chunks of ChunkSize (default value is 4096). Use a smaller size to save memory.
	*/
	AvroBinarySerializer(const avro::ValidSchema &Schema, size_t ChkSize = 4096Ui64) {
		ChunkSize_ = ChkSize;
		out_ = avro::memoryOutputStream(ChkSize);
		e_ = avro::validatingEncoder(Schema, avro::binaryEncoder());
		e_->init(*out_);
	}
	
	~AvroBinarySerializer() = default;

	/*
	* Generic function that makes use of avro::encode() to serialize data.
	*/
	template<class T>
	void Serialize(T& value) {
		avro::encode(*e_, value);
	}

	/*
	* Flush the underlying stream.
	*/
	void Finish() const {
		out_->flush();
	}

	/*
	* Return the vector of serialized data.
	*/
	auto Buffer() const {
		const auto out_stream_length = out_->byteCount();
		const auto inp_stream = avro::memoryInputStream(*out_);
		avro::StreamReader reader(*inp_stream);
		std::vector<uint8_t> data(out_stream_length);
		reader.readBytes(&data[0], out_stream_length);
		return data;
	}

	/*
	* Return the number of bytes written to the output stream. Call this after Finish().
	*/
	auto Size() const {
		return out_->byteCount();
	}

	/*
	* Reset the class so that it can be reused again.
	*/
	void Reset() {
		out_->flush();
		out_.release();
		out_ = avro::memoryOutputStream(ChunkSize_);
		e_->init(*out_);
	}

private:

	std::unique_ptr<avro::OutputStream> out_;
	avro::EncoderPtr e_;
	size_t ChunkSize_ = 4096Ui64;
};





/*
************************ Class to decode binary Avro standard ***********************
*/
class AvroBinaryDeserializer
{
public:

	/*
	* Default class constructor.	
	*/
	AvroBinaryDeserializer(const avro::OutputStream *out) {
		d_ = avro::binaryDecoder();
		in_ = avro::memoryInputStream(*out);
		d_->init(*in_);
	}

	/*
	* Class constructor with Schema validation.	
	*/
	AvroBinaryDeserializer(const avro::ValidSchema& Schema, const avro::OutputStream* out) {
		d_ = avro::validatingDecoder(Schema, avro::binaryDecoder());
		in_ = avro::memoryInputStream(*out);
		d_->init(*in_);
	}

	/*
	* Class constructor with payload and payload size as input arguments.
	*/
	AvroBinaryDeserializer(const uint8_t* data, size_t length) {
		d_ = avro::binaryDecoder();
		in_ = avro::memoryInputStream(data, length);
		d_->init(*in_);
	}

	/*
	* Class constructor with Schema validation, payload, and payload size as input arguments.
	*/
	AvroBinaryDeserializer(const avro::ValidSchema& Schema, const uint8_t* data, size_t length) {
		d_ = avro::validatingDecoder(Schema, avro::binaryDecoder());
		in_ = avro::memoryInputStream(data, length);
		d_->init(*in_);
	}

	~AvroBinaryDeserializer() = default;

	/*
	* Deserialize data and write to the data object.
	*/
	template<class T>
	void Deserialize(T& data_object) {
		avro::decode(*d_, data_object);
	}

private:

	std::unique_ptr<avro::InputStream> in_;
	avro::DecoderPtr d_;
};

