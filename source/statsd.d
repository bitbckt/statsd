module statsd;

import std.random;
import std.socket;

/**
 * The various types of metrics supported by StatsD.
 */
static immutable enum Types {
    Counter = "c",
    Gauge = "g",
    Timing = "ms",
    Set = "s"
}

/**
 * A simple client for the [StatsD](https://github.com/etsy/statsd) protocol.
 */
struct StatsD {

    /**
     * NOTE: Assumes `socket` is in connected state, and that it
     * remain so until this object is destroyed. If the socket is or
     * becomes disconnected, metrics will be dropped.
     *
     * Setting SOCK_NONBLOCK is recommended to prevent blocking
     * metrics-producing threads, and instead dropping metrics.
     */
    this(socket_t socket, string prefix = "", ref Random rng = rndGen()) nothrow {
        this.socket = socket;
        this.prefix = prefix.idup;
        this.rng = rng;

        seed();
    }

    /**
     * Equivalent to `count(key, 1, frequency)`.
     */
    void incr(scope immutable string key, float frequency = 1.0) nothrow {
        count(key, 1, frequency);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.incr("incr");

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixincr:1|c");

        stats.incr("incr", float.max);
        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixincr:1|c");

        stats.seed(42);
        stats.incr("incr", 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixincr:1|c|@0.50");

        stats.incr("incr", 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.incr("incr", 0.0);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.incr("incr", float.nan);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.incr("incr", float.infinity);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixincr:1|c");

        stats.incr("incr", -float.infinity);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");
    }

    /**
     * Equivalent to `count(key, -1, frequency)`.
     */
    void decr(scope immutable string key, float frequency = 1.0) nothrow {
        count(key, -1, frequency);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.decr("decr");

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixdecr:-1|c");

        stats.seed(42);
        stats.decr("decr", 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixdecr:-1|c|@0.50");

        stats.decr("decr", 0.5);

        buf[] = 0;

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.incr("decr", 0.0);
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.decr("decr", float.nan);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.decr("decr", float.infinity);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixdecr:-1|c");

        stats.decr("decr", -float.infinity);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");
    }

    /**
     * Emits a simple counter metric. At each flush, a StatsD daemon
     * will send the current count to an APM, and reset the count to
     * 0.
     */
    void count(scope immutable string key, int delta, float frequency = 1.0) nothrow {
        send(key, delta, Types.Counter, frequency);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.count("count", 42);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixcount:42|c");

        stats.seed(42);
        stats.count("count", 42, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixcount:42|c|@0.50");

        stats.count("count", 42, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.count("count", 42, 0.0);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.count("count", 42, float.nan);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.count("count", 42, float.infinity);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixcount:42|c");

        stats.count("count", 42, -float.infinity);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");
    }

    /**
     * Emits a gauge which maintains its value until it is next set.
     *
     * This implementation does not emit signed gauges (i.e. sending a
     * delta such as -10 or +4).
     */
    void gauge(scope immutable string key, uint value, float frequency = 1.0) nothrow {
        send(key, value, Types.Gauge, frequency);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.gauge("gauge", 128);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixgauge:128|g");

        stats.seed(42);
        stats.gauge("gauge", 128, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixgauge:128|g|@0.50");

        stats.gauge("gauge", 128, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.gauge("gauge", 128, 0.0);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.gauge("gauge", 128, float.nan);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.gauge("gauge", 128, float.infinity);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixgauge:128|g");

        stats.gauge("gauge", 128, -float.infinity);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");
    }

    /**
     * Emits a timing metric in milliseconds. A StatsD daemon will
     * produce a histogram of these timings with a rollup duration
     * equal to its flush interval.
     *
     * It will also maintain a counter with a sample rate equal to the
     * given frequency.
     */
    void timing(scope immutable string key, uint ms, float frequency = 1.0) nothrow {
        send(key, ms, Types.Timing, frequency);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.timing("timing", 2);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixtiming:2|ms");

        stats.seed(42);
        stats.timing("timing", 2, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixtiming:2|ms|@0.50");

        stats.timing("timing", 2, 0.5);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.timing("timing", 2, 0.0);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.timing("timing", 2, float.nan);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");

        stats.timing("timing", 2, float.infinity);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixtiming:2|ms");

        stats.timing("timing", 2, -float.infinity);

        buf[] = 0;
        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "");
    }

    /**
     * Emits a set metric. A StatsD daemon will count unique
     * occurrences of each value between flushes.
     */
    void set(scope immutable string key, uint value) nothrow {
        send(key, value, Types.Set, 1.0);
    }

    unittest {
        import std.string;

        auto pair = socketPair();
        scope(exit) foreach (s; pair) s.close();

        pair[1].blocking(false);

        auto stats = new StatsD(pair[0].handle, "myPrefix");

        auto buf = new ubyte[256];

        stats.set("set", uint.max);

        pair[1].receive(buf);
        assert(fromStringz(cast(char*)buf.ptr) == "myPrefixset:4294967295|s");
    }

    /**
     * (Re-)seeds the random number generator used to sample metrics.
     */
    void seed(uint seed = unpredictableSeed()) pure nothrow @nogc @safe {
        rng.seed(seed);
    }

private:
    immutable socket_t socket;
    immutable string prefix;

    Random rng;

    void send(scope immutable string key, long value, Types type, float frequency) nothrow {
        import std.algorithm.comparison : clamp;
        import std.exception : assumeWontThrow; // Pinky promise.

        version (DigitalMars) {
            import std.math : isClose;
            import std.math.traits : isNaN;
        } else version (LDC) {
            import std.math : approxEqual, isNaN;
            alias approxEqual isClose;
        }

        import std.outbuffer;

        const float freq = clamp(frequency, 0.0, 1.0);

        // Drop everything.
        if (isClose(0.0, freq) || isNaN(freq)) {
            return;
        }

        // Sample if frequency is materially below 1.0.
        const bool closeToOne = isClose(1.0, freq);
        if (!closeToOne && freq < uniform01(rng)) {
            return;
        }

        scope OutBuffer buf = new OutBuffer();
        buf.reserve(256);

        if (closeToOne) {
            assumeWontThrow(buf.writef("%s%s:%d|%r", prefix, key, value, type));
        } else {
            assumeWontThrow(buf.writef("%s%s:%d|%r|@%.2f", prefix, key, value, type, frequency));
        }

        scope ubyte[] bytes = buf.toBytes();

        version (Posix) {
            import core.sys.posix.unistd : write;

            write(socket, bytes.ptr, bytes.length);
        }
    }
}
