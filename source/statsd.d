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

    /**
     * Equivalent to `count(key, -1, frequency)`.
     */
    void decr(scope immutable string key, float frequency = 1.0) nothrow {
        count(key, -1, frequency);
    }

    /**
     * Emits a simple counter metric. At each flush, a StatsD daemon
     * will send the current count to an APM, and reset the count to
     * 0.
     */
    void count(scope immutable string key, int delta, float frequency = 1.0) nothrow {
        send(key, delta, Types.Counter, frequency);
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

    /**
     * Emits a set metric. A StatsD daemon will count unique
     * occurrences of each value between flushes.
     */
    void set(scope immutable string key, uint value) nothrow {
        send(key, value, Types.Set, 1.0);
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
        import std.math : isClose;
        import std.outbuffer;

        const float freq = clamp(frequency, 0.0, 1.0);

        // Drop everything.
        if (isClose(0.0, freq)) {
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
            assumeWontThrow(buf.writef("%s%s:%d|%s|@%.2f", prefix, key, value, type, frequency));
        }

        scope ubyte[] bytes = buf.toBytes();

        version (Posix) {
            import core.sys.posix.unistd : write;

            write(socket, bytes.ptr, bytes.length);
        }
    }
}

unittest {
    import std.stdio;
    import std.string;

    auto pair = socketPair();
    scope(exit) foreach (s; pair) s.close();

    auto stats = new StatsD(pair[0].handle, "myPrefix");

    auto buf = new ubyte[256];

    stats.incr("incr");

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixincr:1|c");

    stats.decr("decr");

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixdecr:-1|c");

    stats.count("count", 42);

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixcount:42|c");

    stats.gauge("gauge", 128);

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixgauge:128|g");

    stats.timing("timing", 2);

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixtiming:2|ms");

    stats.set("set", uint.max);

    pair[1].receive(buf);
    assert(fromStringz(cast(char*)buf.ptr) == "myPrefixset:4294967295|s");
}
