module flod.etc.zlib;

import std.stdio;
import std.exception : enforce;
import etc.c.zlib :
	zlibDeflateInit2 = deflateInit2, zlibDeflate = deflate, zlibDeflateEnd = deflateEnd,
	zlibInflateInit2 = inflateInit2, zlibInflate = inflate, zlibInflateEnd = inflateEnd,
	Z_DEFAULT_COMPRESSION, Z_OK, Z_STREAM_END, Z_STREAM_ERROR, Z_BUF_ERROR, Z_DATA_ERROR,
	Z_FINISH, Z_NO_FLUSH,
	Z_DEFAULT_STRATEGY, Z_HUFFMAN_ONLY, Z_RLE, Z_FILTERED, Z_DEFLATED,
	z_stream;

import flod.pipeline;
import flod.traits;

enum Format {
	raw,
	zlib,
	gzip,
}

enum Level {
	default_ = Z_DEFAULT_COMPRESSION,
	l0 = 0,
	l1 = 1,
	l2 = 2,
	l3 = 3,
	l4 = 4,
	l5 = 5,
	l6 = 6,
	l7 = 7,
	l8 = 8,
	l9 = 9
}

enum Strategy {
	default_ = Z_DEFAULT_STRATEGY,
	rle = Z_RLE,
	huffmanOnly = Z_HUFFMAN_ONLY,
	filtered = Z_FILTERED
}

enum WindowBits {
	b15 = 15,
	b14 = 14,
	b13 = 13,
	b12 = 12,
	b11 = 11,
	b10 = 10,
	b9 = 9,
	b8 = 8,
	default_ = b15
}

enum MemLevel {
	l8 = 8,
	l1 = 1,
	l2 = 2,
	l3 = 3,
	l4 = 4,
	l5 = 5,
	l6 = 6,
	l7 = 7,
	l9 = 9,
	default_ = l8
}

int encodeWindowBits(Format format, WindowBits windowBits)
{
	final switch (format) with(Format) {
	case raw: return -windowBits;
	case gzip: return windowBits + 16;
	case zlib: return windowBits;
	}
}

@filter!(ubyte, ubyte)(Method.peek, Method.pull)
private struct Deflater(alias Context, A...) {
	mixin Context!A;
	private z_stream stream;

	this()(int level, int windowBits, int memLevel, int strategy)
	{
		stream.zalloc = null;
		stream.zfree = null;
		stream.opaque = null;
		int result = zlibDeflateInit2(&stream, level, Z_DEFLATED, windowBits, memLevel, strategy);
		enforce(result == Z_OK, "deflateInit failed");
	}

	~this()
	{
		zlibDeflateEnd(&stream);
	}

	size_t pull()(ubyte[] buf)
	{
		stream.next_out = buf.ptr;
		stream.avail_out = cast(uint) buf.length;
		while (stream.avail_out) {
			auto ib = source.peek(buf.length);
			auto flush = ib.length < buf.length ? Z_FINISH : Z_NO_FLUSH;
			auto avail = cast(uint) min(uint.max, ib.length);
			ubyte dummy;
			stream.next_in = ib.ptr == null ? &dummy : ib.ptr;
			stream.avail_in = ib.ptr == null ? 0 : avail;
			int result = zlibDeflate(&stream, flush);
			enforce(result != Z_STREAM_ERROR, "deflate: stream error");
			auto cons = avail - stream.avail_in;
			if (cons > 0)
				source.consume(cons);
			if (result == Z_STREAM_END || result == Z_BUF_ERROR)
				break;
		}
		return buf.length - stream.avail_out;
	}
}

///
auto deflate(S)(auto ref S schema, Format format = Format.zlib, Level level = Level.default_,
	WindowBits windowBits = WindowBits.default_, MemLevel memLevel = MemLevel.default_,
	Strategy strategy = Strategy.default_)
	if (isSchema!S)
{
	return schema.pipe!Deflater(level, encodeWindowBits(format, windowBits), memLevel, strategy);
}

///
auto deflate(S)(auto ref S schema, Level level,
	WindowBits windowBits = WindowBits.default_, MemLevel memLevel = MemLevel.default_,
	Strategy strategy = Strategy.default_)
	if (isSchema!schema)
{
	return deflate(schema, Format.zlib, level, windowBits, memLevel, strategy);
}

@filter!(ubyte, ubyte)(Method.peek, Method.pull)
private struct Inflater(alias Context, A...) {
	mixin Context!A;
	private z_stream stream;

	this()(int windowBits)
	{
		stream.zalloc = null;
		stream.zfree = null;
		stream.opaque = null;
		int result = zlibInflateInit2(&stream, windowBits);
		enforce(result == Z_OK, "inflateInit2 failed");
	}

	~this()
	{
		enforce(zlibInflateEnd(&stream) == Z_OK, "inflateEnd failed");
	}

	size_t pull()(ubyte[] buf)
	{
		stream.next_out = buf.ptr;
		stream.avail_out = cast(uint) buf.length;
		size_t offs = 0;
		int result = Z_STREAM_END;
		while (stream.avail_out) {
			auto ib = source.peek(buf.length);
			if (ib.length == 0)
				break;
			auto avail = cast(uint) min(uint.max, ib.length);
			stream.next_in = ib.ptr;
			stream.avail_in = avail;
			result = zlibInflate(&stream, Z_NO_FLUSH);
			if (result == Z_STREAM_ERROR)
				throw new Exception("inflate: stream error");
			if (result == Z_DATA_ERROR)
				throw new Exception("inflate: data error");
			auto cons = avail - stream.avail_in;
			if (cons > 0)
				source.consume(cons);
			if (result == Z_STREAM_END)
				break;
			if (result == Z_BUF_ERROR)
				break;
		}
		return buf.length - stream.avail_out;
	}
}

///
auto inflate(S)(auto ref S schema, Format format, WindowBits windowBits = WindowBits.default_)
	if (isSchema!S)
{
	return schema.pipe!Inflater(encodeWindowBits(format, windowBits));
}

///
auto inflate(S)(auto ref S pipeline)
	if (isSchema!S)
{
	return schema.pipe!Inflater(0);
}

unittest
{
	import std.array : appender;
	import std.stdio;
	import std.range : retro, NullSink;
	import flod : copy;

	foreach (i; [0, 1, 2, 3, 4, 5, 10, 16, 4095, 4096, 8192, 16384, 32768, 65536, 1048577, 132 * 1048576]) {
		import std.range : take, repeat;
		auto w = appender!(ubyte[]);
		auto n = appender!(ubyte[]);
		repeat(ubyte(0)).take(i).deflate(Format.zlib).copy(n);
		writeln(n.data.length);
		repeat(ubyte(0)).take(i).deflate(Format.gzip).inflate(Format.gzip).copy(w); //NullSink());
		assert(w.data.length == i);
	}
}
