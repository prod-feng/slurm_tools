#!/bin/bash

# Configuration
PORT=8777  #change the port
BIND_IP="x.x.10.211" #Change the IP
LOG_FILE="server.log"
DIR="." # Serves current directory. Change to a path like "/var/www" if needed.

# Function to start the server
start_server() {
    # Check if already running
    if pgrep -f "python3 -m http.server $PORT" > /dev/null; then
        echo "Server is already running!"
        exit 1
    fi

    echo "Starting Python HTTP server on $BIND_IP:$PORT..."
    # Run in background and save logs
    nohup python3 -m http.server $PORT --bind $BIND_IP --directory "$DIR" > "$LOG_FILE" 2>&1 &
    
    # Wait a brief moment and check if it successfully started
    sleep 1
    if pgrep -f "python3 -m http.server $PORT" > /dev/null; then
        echo "Server started successfully in the background."
        echo "Logs are being saved to: $LOG_FILE"
    else
        echo "Failed to start server. Check $LOG_FILE for errors."
    fi
}

# Function to stop the server
stop_server() {
    PID=$(pgrep -f "python3 -m http.server $PORT")
    if [ -z "$PID" ]; then
        echo "Server is not running."
    else
        echo "Stopping server (PID: $PID)..."
        kill "$PID"
        echo "Server stopped."
    fi
}

# Function to check status
status_server() {
    PID=$(pgrep -f "python3 -m http.server $PORT")
    if [ -z "$PID" ]; then
        echo "Server status: STOPPED"
    else
        echo "Server status: RUNNING (PID: $PID)"
        echo "Listening on: $BIND_IP:$PORT"
    fi
}

# Check command line arguments
case "$1" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    status)
        status_server
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac


