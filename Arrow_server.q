/-------------------------------------------
/ arrowServer.q – kdb+ Startup Script
/-------------------------------------------

/ --- Parameters ---
port: 5001;  / Default port for incoming connections
arrowChunkRows: 100000;  / Default number of rows per Arrow record batch
compression: `UNCOMPRESSED;  / Default compression setting (supported: UNCOMPRESSED, ZSTD, LZ4)
arrowOptions: (``ARROW_CHUNK_ROWS)!((::); arrowChunkRows);

/ Define a sample table if one isn’t already defined.
if[not `myTable in key `.;
    myTable: ([] int_field: 1 2 3;
                float_field: 1.0 2.0 3.0;
                str_field: ("a"; "b"; "c"))
];

/ --- Helper Function --- 
/ Convert a 64-bit integer to an 8-byte big-endian byte list.
pow256: 1 256 65536 16777216 4294967296 1099511627776 281474976710656 72057594037927936;
to8ByteHeader:{[n]
    / For each factor in pow256, compute n div factor mod 256, then reverse the list for big-endian order.
    : reverse {n div x mod 256} each pow256
};

/ --- Arrow Serialization Wrapper ---
serializeArrowTable:{[tbl; opts]
    / Wrap the arrowkdb serialization call.
    .arrowkdb.ipc.serializeArrowFromTable[tbl; opts]
};

/ --- Raw Socket Handler (.z.w) ---
/ This handler is invoked when the server receives a raw (unpacked) message.
.z.w:{[h; msg]
    / If the message is exactly "getArrowData", process it.
    if[msg = "getArrowData";
        / Serialize the table (using arrowOptions, which includes chunking)
        arrowData: serializeArrowTable[myTable; arrowOptions];
        / Create an 8-byte header for the total message length
        header: to8ByteHeader[count arrowData];
        / Combine header and Arrow IPC stream into one message
        framedMessage: header, arrowData;
        / Send the framed message over the socket
        h framedMessage;
        : 0
    ];
    : "Unrecognized command"
};

/ --- Optional: Synchronous Query Handler (.z.pg) ---
/ This handler supports queries sent as a packed group: (function; parameters)
.z.pg:{[h; msg]
    if[2=count msg;
        result: (first msg) (last msg);
        if[tableQ result;
            arrowData: serializeArrowTable[result; arrowOptions];
            header: to8ByteHeader[count arrowData];
            framedMessage: header, arrowData;
            h framedMessage;
            : 0
        ];
        : result
    ];
    : "Unsupported message format"
};

/ --- Start Listening ---
\p port
