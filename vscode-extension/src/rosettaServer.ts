/**
 * Rosetta HTTP Server manager.
 *
 * Spawns `python -m rosetta.serve` as a headless background process
 * (no interactive REPL) that exposes the REST API + Playground.
 */

import * as vscode from 'vscode';
import * as http from 'http';
import { ChildProcess, spawn } from 'child_process';
import * as path from 'path';
import * as os from 'os';
import { RosettaCLI } from './rosettaCli';

export class RosettaServer {
    private _process: ChildProcess | null = null;
    private _port: number = 0;
    private _cli: RosettaCLI;
    private _outputChannel: vscode.OutputChannel;

    constructor(cli: RosettaCLI, outputChannel: vscode.OutputChannel) {
        this._cli = cli;
        this._outputChannel = outputChannel;
    }

    get port(): number { return this._port; }
    get running(): boolean { return this._process !== null && !this._process.killed; }
    get baseUrl(): string { return `http://localhost:${this._port}`; }

    /** Start the headless Rosetta server. */
    async start(): Promise<void> {
        if (this.running) { return; }

        const config = vscode.workspace.getConfiguration('rosetta');
        const pythonPath = config.get<string>('pythonPath') ?? 'python3';
        let port = config.get<number>('serverPort') ?? 0;

        if (port === 0) {
            port = await this._findFreePort();
        }

        const configPath = this._cli.getConfigPath();
        const cwd = path.dirname(configPath);

        this._outputChannel.appendLine(`Starting Rosetta server: ${pythonPath} -m rosetta.serve -p ${port}`);

        this._process = spawn(pythonPath, [
            '-m', 'rosetta.serve',
            '-c', configPath,
            '-p', String(port),
            '-o', path.join(os.homedir(), '.rosetta', 'results'),
        ], {
            cwd,
            stdio: ['pipe', 'pipe', 'pipe'],
        });

        this._port = port;

        // Parse the JSON line printed by serve.py to get actual port
        const portPromise = new Promise<number>((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Server did not print port within 15s')), 15000);
            let stdoutBuf = '';

            this._process!.stdout?.on('data', (data: Buffer) => {
                const text = data.toString();
                stdoutBuf += text;
                this._outputChannel.appendLine(`[server] ${text.trim()}`);

                // Try to parse the JSON line with port info
                for (const line of stdoutBuf.split('\n')) {
                    try {
                        const info = JSON.parse(line.trim());
                        if (info.port) {
                            clearTimeout(timeout);
                            resolve(info.port);
                            return;
                        }
                    } catch { /* not JSON yet */ }
                }
            });

            this._process!.on('exit', (code) => {
                clearTimeout(timeout);
                reject(new Error(`Server process exited with code ${code}`));
            });
        });

        this._process.stderr?.on('data', (data: Buffer) => {
            this._outputChannel.appendLine(`[server:err] ${data.toString().trim()}`);
        });

        this._process.on('exit', (code) => {
            this._outputChannel.appendLine(`[server] Process exited with code ${code}`);
            this._process = null;
        });

        // Wait for port from stdout, then verify HTTP is ready
        try {
            this._port = await portPromise;
            await this._waitForReady(this._port, 10000);
            this._outputChannel.appendLine(`Rosetta server ready on port ${this._port}`);
        } catch (e) {
            this.stop();
            throw e;
        }
    }

    /** Stop the server. */
    stop(): void {
        if (this._process && !this._process.killed) {
            this._process.kill('SIGTERM');
            this._process = null;
            this._outputChannel.appendLine('Rosetta server stopped.');
        }
    }

    // -----------------------------------------------------------------
    // HTTP API helpers
    // -----------------------------------------------------------------

    async get(apiPath: string): Promise<any> {
        return this._request('GET', apiPath);
    }

    async post(apiPath: string, body?: any): Promise<any> {
        return this._request('POST', apiPath, body);
    }

    async executeStream(
        sql: string,
        dbms?: string[],
        onProgress?: (event: string, data: any) => void,
    ): Promise<void> {
        const body = JSON.stringify({ sql, dbms: dbms ?? [] });
        return new Promise((resolve, reject) => {
            const req = http.request({
                hostname: 'localhost', port: this._port,
                path: '/api/execute/stream', method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
            }, (res) => {
                let buffer = '';
                res.on('data', (chunk: Buffer) => {
                    buffer += chunk.toString();
                    const parts = buffer.split('\n\n');
                    buffer = parts.pop() ?? '';
                    for (const part of parts) {
                        const eventMatch = part.match(/^event:\s*(.+)$/m);
                        const dataMatch = part.match(/^data:\s*(.+)$/m);
                        if (eventMatch && dataMatch) {
                            try { onProgress?.(eventMatch[1], JSON.parse(dataMatch[1])); }
                            catch { /* ignore */ }
                        }
                    }
                });
                res.on('end', () => resolve());
                res.on('error', reject);
            });
            req.on('error', reject);
            req.write(body);
            req.end();
        });
    }

    // -----------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------

    private _request(method: string, apiPath: string, body?: any): Promise<any> {
        return new Promise((resolve, reject) => {
            const bodyStr = body ? JSON.stringify(body) : undefined;
            const req = http.request({
                hostname: 'localhost', port: this._port, path: apiPath, method,
                headers: {
                    'Content-Type': 'application/json',
                    ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
                },
            }, (res) => {
                let data = '';
                res.on('data', (chunk: Buffer) => { data += chunk.toString(); });
                res.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve(data); } });
                res.on('error', reject);
            });
            req.on('error', reject);
            if (bodyStr) { req.write(bodyStr); }
            req.end();
        });
    }

    private _findFreePort(): Promise<number> {
        return new Promise((resolve, reject) => {
            const srv = require('net').createServer();
            srv.listen(0, () => {
                const port = (srv.address() as any).port;
                srv.close(() => resolve(port));
            });
            srv.on('error', reject);
        });
    }

    private _waitForReady(port: number, timeoutMs: number): Promise<void> {
        const start = Date.now();
        return new Promise((resolve, reject) => {
            const check = () => {
                if (Date.now() - start > timeoutMs) {
                    reject(new Error(`Server HTTP not ready within ${timeoutMs}ms`));
                    return;
                }
                const req = http.get(`http://localhost:${port}/api/dbms`, (res) => {
                    res.resume();
                    if (res.statusCode === 200) { resolve(); }
                    else { setTimeout(check, 300); }
                });
                req.on('error', () => setTimeout(check, 300));
            };
            setTimeout(check, 500);
        });
    }
}
