#!/bin/bash

echo "Restarting servers..."

echo "Stopping Go server..."
pkill -f "go run main.go" || pkill -f "main.go" || true
lsof -ti:8080 | xargs kill -9 2>/dev/null || true

echo "Stopping Vite server..."
pkill -f "vite" || true
lsof -ti:5173 | xargs kill -9 2>/dev/null || true

sleep 2

echo "Servers stopped"
echo ""
echo "Now run: pnpm start"

