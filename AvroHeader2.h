/*
   PROGRAM: AVRO Serializer and Deserializer
   AUTHOR:  Afshin Ahmadi
   DATE:    2023-09-09
   VER:		1.0
   HELP:    https://avro.apache.org/docs/1.11.1/api/cpp/html/
*/

#pragma once

#include <vector>
#include <stdexcept>  // For standard exceptions
#include <iostream>   // For logging errors (optional)

#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"
#include "avro/Serializer.hh"
#include "avro/Writer.hh"

/*
************************ Custom Exception for Avro Errors ************************
*/
class AvroException : public std::runtime_error {
public:
    explicit AvroException(const std::string& message) : std::runtime_error(message) {}
};

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
    AvroBinarySerializer(size_t ChkSize = 4096U) {
        try {
            ChunkSize_ = ChkSize;
            out_ = avro::memoryOutputStream(ChkSize);
            e_ = avro::binaryEncoder();
            e_->init(*out_);
        } catch (const std::exception& e) {
            throw AvroException("Error during serializer initialization: " + std::string(e.what()));
        }
    }
    
    /*
    * Class constructor with Schema validation.
    * OutputStream grows in memory chunks of ChunkSize (default value is 4096). Use a smaller size to save memory.
    */
    AvroBinarySerializer(const avro::ValidSchema &Schema, size_t ChkSize = 4096U) {
        try {
            ChunkSize_ = ChkSize;
            out_ = avro::memoryOutputStream(ChkSize);
            e_ = avro::validatingEncoder(Schema, avro::binaryEncoder());
            e_->init(*out_);
            schema_ = Schema;  // Store the schema for later retrieval
        } catch (const std::exception& e) {
            throw AvroException("Error during serializer initialization with schema: " + std::string(e.what()));
        }
    }

    // Move constructor
    AvroBinarySerializer(AvroBinarySerializer&& other) noexcept
        : out_(std::move(other.out_)), e_(std::move(other.e_)), ChunkSize_(other.ChunkSize_), schema_(std::move(other.schema_)) {
        other.ChunkSize_ = 0;
    }

    // Move assignment operator
    AvroBinarySerializer& operator=(AvroBinarySerializer&& other) noexcept {
        if (this != &other) {
            out_ = std::move(other.out_);
            e_ = std::move(other.e_);
            ChunkSize_ = other.ChunkSize_;
            schema_ = std::move(other.schema_);
            other.ChunkSize_ = 0;
        }
        return *this;
    }

    ~AvroBinarySerializer() = default;

    /*
    * Generic function that makes use of avro::encode() to serialize data.
    */
    template<class T>
    void Serialize(T& value) {
        try {
            avro::encode(*e_, value);
        } catch (const std::exception& e) {
            throw AvroException("Error during serialization: " + std::string(e.what()));
        }
    }

    /*
    * Flush the underlying stream.
    */
    void Finish() const {
        try {
            out_->flush();
        } catch (const std::exception& e) {
            throw AvroException("Error flushing the stream: " + std::string(e.what()));
        }
    }

    /*
    * Return the vector of serialized data (with move semantics).
    */
    std::vector<uint8_t> Buffer() && {
        try {
            const auto out_stream_length = out_->byteCount();
            const auto inp_stream = avro::memoryInputStream(*out_);
            avro::StreamReader reader(*inp_stream);
            std::vector<uint8_t> data(out_stream_length);
            reader.readBytes(&data[0], out_stream_length);
            return std::move(data);  // Move the vector to avoid copying
        } catch (const std::exception& e) {
            throw AvroException("Error creating buffer from the serialized data: " + std::string(e.what()));
        }
    }

    /*
    * Return the number of bytes written to the output stream. Call this after Finish().
    */
    size_t Size() const {
        return out_->byteCount();
    }

    /*
    * Reset the class so that it can be reused again.
    */
    void Reset() {
        try {
            out_->flush();
            out_ = avro::memoryOutputStream(ChunkSize_);
            e_->init(*out_);
        } catch (const std::exception& e) {
            throw AvroException("Error during reset: " + std::string(e.what()));
        }
    }

    // Peek the encoder's byte count (how much has been serialized so far)
    size_t PeekEncoder() const {
        return out_->byteCount();
    }

    // Retrieve the schema used for validation
    avro::ValidSchema GetSchema() const {
        return schema_;
    }

private:
    std::unique_ptr<avro::OutputStream> out_;
    avro::EncoderPtr e_;
    size_t ChunkSize_ = 4096U;
    avro::ValidSchema schema_;  // Store the schema used for validation
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
        try {
            d_ = avro::binaryDecoder();
            in_ = avro::memoryInputStream(*out);
            d_->init(*in_);
        } catch (const std::exception& e) {
            throw AvroException("Error during deserializer initialization: " + std::string(e.what()));
        }
    }

    /*
    * Class constructor with Schema validation.	
    */
    AvroBinaryDeserializer(const avro::ValidSchema& Schema, const avro::OutputStream* out) {
        try {
            d_ = avro::validatingDecoder(Schema, avro::binaryDecoder());
            in_ = avro::memoryInputStream(*out);
            d_->init(*in_);
            schema_ = Schema;  // Store the schema for later retrieval
        } catch (const std::exception& e) {
            throw AvroException("Error during deserializer initialization with schema: " + std::string(e.what()));
        }
    }

    /*
    * Class constructor with payload and payload size as input arguments.
    */
    AvroBinaryDeserializer(const uint8_t* data, size_t length) {
        try {
            d_ = avro::binaryDecoder();
            in_ = avro::memoryInputStream(data, length);
            d_->init(*in_);
        } catch (const std::exception& e) {
            throw AvroException("Error during deserialization initialization: " + std::string(e.what()));
        }
    }

    /*
    * Class constructor with Schema validation, payload, and payload size as input arguments.
    */
    AvroBinaryDeserializer(const avro::ValidSchema& Schema, const uint8_t* data, size_t length) {
        try {
            d_ = avro::validatingDecoder(Schema, avro::binaryDecoder());
            in_ = avro::memoryInputStream(data, length);
            d_->init(*in_);
            schema_ = Schema;  // Store the schema for later retrieval
        } catch (const std::exception& e) {
            throw AvroException("Error during deserialization initialization with schema: " + std::string(e.what()));
        }
    }

    // Move constructor
    AvroBinaryDeserializer(AvroBinaryDeserializer&& other) noexcept
        : in_(std::move(other.in_)), d_(std::move(other.d_)), schema_(std::move(other.schema_)) {}

    // Move assignment operator
    AvroBinaryDeserializer& operator=(AvroBinaryDeserializer&& other) noexcept {
        if (this != &other) {
            in_ = std::move(other.in_);
            d_ = std::move(other.d_);
            schema_ = std::move(other.schema_);
        }
        return *this;
    }

    ~AvroBinaryDeserializer() = default;

    /*
    * Deserialize data and write to the data object.
    */
    template<class T>
    void Deserialize(T& data_object) {
        try {
            avro::decode(*d_, data_object);
        } catch (const std::exception& e) {
            throw AvroException("Error during deserialization: " + std::string(e.what()));
        }
    }

    // Peek the decoder's byte count (how much data has been read so far)
    size_t PeekDecoder() const {
        return in_->byteCount();
    }

    // Retrieve the schema used for validation
    avro::ValidSchema GetSchema() const {
        return schema_;
    }

private:
    std::unique_ptr<avro::InputStream> in_;
    avro::DecoderPtr d_;
    avro::ValidSchema schema_;  // Store the schema used for validation
};
