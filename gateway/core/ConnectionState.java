package gateway.core;

public enum ConnectionState {
    AWAITING_HEADERS,
    WRITING_RESPONSE,
    UPGRADED,
    CLOSED
}