/**
 * Command registrations for the Rosetta extension.
 */

import * as vscode from 'vscode';
import * as path from 'path';
import * as os from 'os';
import * as fs from 'fs';
import { RosettaCLI } from './rosettaCli';
import { RosettaServer } from './rosettaServer';
import { DbmsTreeProvider } from './views/dbmsTree';
import { HistoryTreeProvider } from './views/historyTree';
import { showReportWebview, showExecResultWebview } from './views/reportWebview';

export function registerCommands(
    context: vscode.ExtensionContext,
    cli: RosettaCLI,
    server: RosettaServer,
    dbmsTree: DbmsTreeProvider,
    historyTree: HistoryTreeProvider,
    outputChannel: vscode.OutputChannel,
    statusBar: vscode.StatusBarItem,
): void {
    const reg = (id: string, handler: (...args: any[]) => any) => {
        context.subscriptions.push(vscode.commands.registerCommand(id, handler));
    };

    /**
     * Helper: pick DBMS targets.
     * Uses cached data from the DBMS tree to avoid an extra CLI call.
     * Falls back to a CLI call if cache is empty.
     */
    async function pickDbms(): Promise<string | undefined> {
        let connected = dbmsTree.connectedDbms;

        // If cache is empty, do a quick refresh
        if (connected.length === 0) {
            statusBar.text = '$(sync~spin) Rosetta';
            try {
                const result = await cli.status();
                if (result.ok && result.data?.dbms) {
                    connected = (result.data.dbms as any[]).filter((d: any) => d.connected);
                }
            } catch { /* ignore */ }
        }

        if (connected.length === 0) {
            vscode.window.showErrorMessage('No DBMS connected. Check your config and connections.');
            return undefined;
        }

        if (connected.length === 1) {
            return connected[0].name;
        }

        const picks = connected.map(d => ({ label: d.name, picked: true }));
        const selected = await vscode.window.showQuickPick(picks, {
            canPickMany: true,
            title: 'Select DBMS targets',
            placeHolder: 'Choose which databases to run against',
        });

        if (!selected || selected.length === 0) { return undefined; }
        return selected.map(s => s.label).join(',');
    }

    // -----------------------------------------------------------------
    // Refresh DBMS status (triggers onDidRefresh → history + status bar)
    // -----------------------------------------------------------------
    reg('rosetta.refreshStatus', () => {
        statusBar.text = '$(sync~spin) Rosetta';
        dbmsTree.refresh();
    });

    // -----------------------------------------------------------------
    // Run MTR test
    // -----------------------------------------------------------------
    reg('rosetta.runTest', async (uriOrArgs?: vscode.Uri) => {
        let testFile: string | undefined;

        if (uriOrArgs instanceof vscode.Uri) {
            testFile = uriOrArgs.fsPath;
        } else {
            const files = await vscode.window.showOpenDialog({
                canSelectMany: false,
                filters: { 'MTR Test Files': ['test'] },
                title: 'Select .test file',
            });
            testFile = files?.[0]?.fsPath;
        }
        if (!testFile) { return; }

        const dbms = await pickDbms();
        if (!dbms) { return; }

        await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: `MTR: ${path.basename(testFile)}`,
                cancellable: false,
            },
            async (progress) => {
                progress.report({ increment: 0, message: `on ${dbms}...` });
                let lastPct = 0;
                try {
                    const mtrArgs = ['mtr', '-t', testFile!, '--dbms', dbms];
                    const result = await cli.execWithProgress(mtrArgs, (pct, msg) => {
                        const increment = pct - lastPct;
                        if (increment > 0) {
                            lastPct = pct;
                            progress.report({ increment, message: msg });
                        }
                    });
                    if (result.ok && result.data) {
                        const reportDir = result.data.report_directory;
                        if (reportDir && fs.existsSync(reportDir)) {
                            const htmlFiles = fs.readdirSync(reportDir).filter(f => f.endsWith('.html'));
                            if (htmlFiles.length > 0) {
                                showReportWebview(
                                    path.join(reportDir, htmlFiles[0]),
                                    `MTR: ${path.basename(testFile!)}`,
                                );
                            }
                        }
                        vscode.window.showInformationMessage(`MTR complete: ${path.basename(testFile!)}`);
                    } else {
                        vscode.window.showErrorMessage(`MTR failed: ${result.error ?? 'Unknown error'}`);
                    }
                } catch (e: any) {
                    vscode.window.showErrorMessage(`MTR error: ${e.message}`);
                }
                historyTree.refresh();
            },
        );
    });

    // -----------------------------------------------------------------
    // Run benchmark
    // -----------------------------------------------------------------
    reg('rosetta.runBenchmark', async (uri?: vscode.Uri) => {
        let benchFile: string | undefined;

        if (uri instanceof vscode.Uri) {
            benchFile = uri.fsPath;
        } else {
            const files = await vscode.window.showOpenDialog({
                canSelectMany: false,
                filters: { 'Benchmark Files': ['json', 'sql'] },
                title: 'Select benchmark file',
            });
            benchFile = files?.[0]?.fsPath;
        }
        if (!benchFile) { return; }

        const dbms = await pickDbms();
        if (!dbms) { return; }

        await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: `Bench: ${path.basename(benchFile)}`,
                cancellable: false,
            },
            async (progress) => {
                progress.report({ increment: 0, message: `on ${dbms}...` });
                let lastPct = 0;
                try {
                    const benchArgs = ['bench', '--file', benchFile!, '--dbms', dbms];
                    const result = await cli.execWithProgress(benchArgs, (pct, msg) => {
                        const increment = pct - lastPct;
                        if (increment > 0) {
                            lastPct = pct;
                            progress.report({ increment, message: msg });
                        }
                    });
                    if (result.ok && result.data) {
                        const reportDir = result.data.report_directory;
                        if (reportDir && fs.existsSync(reportDir)) {
                            const htmlFiles = fs.readdirSync(reportDir).filter(f => f.endsWith('.html'));
                            if (htmlFiles.length > 0) {
                                showReportWebview(
                                    path.join(reportDir, htmlFiles[0]),
                                    `Bench: ${path.basename(benchFile!)}`,
                                );
                            }
                        }
                        vscode.window.showInformationMessage(`Benchmark complete: ${path.basename(benchFile!)}`);
                    } else {
                        vscode.window.showErrorMessage(`Benchmark failed: ${result.error ?? 'Unknown error'}`);
                    }
                } catch (e: any) {
                    vscode.window.showErrorMessage(`Benchmark error: ${e.message}`);
                }
                historyTree.refresh();
            },
        );
    });

    // -----------------------------------------------------------------
    // Execute SQL
    // -----------------------------------------------------------------
    reg('rosetta.executeSQL', async () => {
        const editor = vscode.window.activeTextEditor;
        let sql = editor?.document.getText(editor.selection);
        if (!sql || sql.trim() === '') {
            sql = editor?.document.getText();
        }
        if (!sql || sql.trim() === '') {
            sql = await vscode.window.showInputBox({
                prompt: 'Enter SQL to execute across all DBMS',
                placeHolder: 'SELECT VERSION()',
            });
        }
        if (!sql) { return; }

        await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: 'Executing SQL...',
                cancellable: false,
            },
            async () => {
                try {
                    const result = await cli.execSQL(sql!);
                    if (result.ok && result.data) {
                        showExecResultWebview(result.data);
                    } else {
                        vscode.window.showErrorMessage(`SQL failed: ${result.error ?? 'Unknown error'}`);
                    }
                } catch (e: any) {
                    vscode.window.showErrorMessage(`SQL error: ${e.message}`);
                }
            },
        );
    });

    // -----------------------------------------------------------------
    // Open SQL Playground (Webview + message proxy to bypass CSP)
    // -----------------------------------------------------------------
    reg('rosetta.openPlayground', async () => {
        if (!server.running) {
            try {
                await vscode.window.withProgress(
                    { location: vscode.ProgressLocation.Notification, title: 'Starting Rosetta server...' },
                    () => server.start(),
                );
            } catch (e: any) {
                vscode.window.showErrorMessage(`Failed to start server: ${e.message}`);
                return;
            }
        }

        const configDir = path.dirname(cli.getConfigPath());
        const playgroundPath = path.join(os.homedir(), '.rosetta', 'results', 'playground.html');

        if (!fs.existsSync(playgroundPath)) {
            vscode.window.showErrorMessage(`playground.html not found. Run a test first to generate it.`);
            return;
        }

        let html = fs.readFileSync(playgroundPath, 'utf-8');

        const serverBase = `http://localhost:${server.port}`;
        html = html.replace(
            /const port = location\.port \|\| '80';\s*\n\s*const base = location\.protocol \+ '\/\/' \+ location\.hostname \+ ':' \+ port;/g,
            `const port = '${server.port}'; const base = '${serverBase}';`,
        );

        html = html.replace(/<meta[^>]*Content-Security-Policy[^>]*>/gi, '');

        // Hide navigation buttons that don't work in Webview
        const hideNav = '<style>a[href*="index.html"],a[href*="playground.html"],a[href*="whitelist.html"],a[href*="buglist.html"]{display:none!important}</style>';
        html = html.replace('</head>', hideNav + '\n</head>');

        // Inject fetch proxy — using a self-executing function string to avoid escaping issues
        const proxyCode = [
            '<script>',
            '(function(){',
            '  var vscode = acquireVsCodeApi();',
            '  var _orig = window.fetch;',
            '  var _map = {};',
            '  var _id = 0;',
            '  window.fetch = function(url, opts) {',
            '    var u = (typeof url === "string") ? url : url.toString();',
            '    if (u.indexOf("/api/") === -1) return _orig.apply(this, arguments);',
            '    var id = ++_id;',
            '    return new Promise(function(resolve, reject) {',
            '      _map[id] = {resolve: resolve, reject: reject};',
            '      vscode.postMessage({type:"fetch", id:id, url:u, method:(opts&&opts.method)||"GET", body:(opts&&opts.body)||null});',
            '    });',
            '  };',
            '  window.addEventListener("message", function(e) {',
            '    var m = e.data;',
            '    if (m.type === "fetchResponse" && _map[m.id]) {',
            '      var p = _map[m.id]; delete _map[m.id];',
            '      var b = (typeof m.body === "string") ? m.body : JSON.stringify(m.body);',
            '      p.resolve(new Response(b, {status: m.status||200, headers:{"Content-Type":"application/json"}}));',
            '    }',
            '    if (m.type === "fetchError" && _map[m.id]) {',
            '      var p = _map[m.id]; delete _map[m.id];',
            '      p.reject(new Error(m.error));',
            '    }',
            '    if (m.type === "sseEvent" && _map[m.id]) {',
            '      var p = _map[m.id];',
            '      if (!p.ctrl) {',
            '        var stream = new ReadableStream({start: function(c){p.ctrl=c; _pushSSE(p,m);}});',
            '        p.resolve(new Response(stream, {status:200, headers:{"Content-Type":"text/event-stream"}}));',
            '      } else { _pushSSE(p,m); }',
            '    }',
            '  });',
            '  function _pushSSE(p,m) {',
            '    var txt = "event: "+m.event+"\\ndata: "+JSON.stringify(m.data)+"\\n\\n";',
            '    p.ctrl.enqueue(new TextEncoder().encode(txt));',
            '    if (m.event==="done"||m.event==="error"||m.event==="cancelled") {',
            '      p.ctrl.close(); delete _map[m.id];',
            '    }',
            '  }',
            '})();',
            '</script>',
        ].join('\n');
        html = html.replace('<head>', '<head>\n' + proxyCode);

        const panel = vscode.window.createWebviewPanel(
            'rosettaPlayground', 'Rosetta Playground', vscode.ViewColumn.One,
            { enableScripts: true, retainContextWhenHidden: true },
        );

        // Extension-side: handle proxied fetch requests via Node http
        const http = require('http') as typeof import('http');

        panel.webview.onDidReceiveMessage(async (msg: any) => {
            if (msg.type !== 'fetch') { return; }

            const urlPath = new URL(msg.url).pathname;
            const isStream = urlPath.includes('/stream');
            const bodyStr: string = msg.body || '';

            try {
                if (isStream) {
                    const req = http.request({
                        hostname: 'localhost', port: server.port,
                        path: urlPath, method: msg.method || 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            ...(bodyStr ? { 'Content-Length': String(Buffer.byteLength(bodyStr)) } : {}),
                        },
                    }, (res) => {
                        let buf = '';
                        res.on('data', (chunk: Buffer) => {
                            buf += chunk.toString();
                            const parts = buf.split('\n\n');
                            buf = parts.pop() || '';
                            for (const part of parts) {
                                const evtM = part.match(/^event:\s*(.+)$/m);
                                const dataM = part.match(/^data:\s*(.+)$/m);
                                if (evtM && dataM) {
                                    try {
                                        panel.webview.postMessage({
                                            type: 'sseEvent', id: msg.id,
                                            event: evtM[1], data: JSON.parse(dataM[1]),
                                        });
                                    } catch { /* ignore parse errors */ }
                                }
                            }
                        });
                        res.on('end', () => {
                            panel.webview.postMessage({
                                type: 'sseEvent', id: msg.id,
                                event: 'done', data: { ok: true },
                            });
                        });
                    });
                    req.on('error', (e: Error) => {
                        panel.webview.postMessage({ type: 'fetchError', id: msg.id, error: e.message });
                    });
                    if (bodyStr) { req.write(bodyStr); }
                    req.end();
                } else {
                    // Regular request — use server helper
                    const result = await server[msg.method === 'POST' ? 'post' : 'get'](
                        urlPath, msg.body ? JSON.parse(msg.body) : undefined,
                    );
                    panel.webview.postMessage({
                        type: 'fetchResponse', id: msg.id,
                        status: 200, body: result,
                    });
                }
            } catch (e: any) {
                panel.webview.postMessage({ type: 'fetchError', id: msg.id, error: e.message });
            }
        }, undefined, context.subscriptions);

        panel.webview.html = html;
    });

    // -----------------------------------------------------------------
    // View / refresh history
    // -----------------------------------------------------------------
    reg('rosetta.viewHistory', () => {
        historyTree.refresh();
    });

    // -----------------------------------------------------------------
    // Show report — FAST path: direct file load (from history click)
    // -----------------------------------------------------------------
    reg('rosetta.showReportFile', (htmlPath?: string, title?: string) => {
        if (htmlPath && fs.existsSync(htmlPath)) {
            showReportWebview(htmlPath, title ?? 'Rosetta Report');
        } else {
            vscode.window.showWarningMessage(
                `Report file not found: ${htmlPath ?? 'unknown'}`,
            );
        }
    });

    // -----------------------------------------------------------------
    // Show report — SLOW path: CLI lookup by run ID (from command palette)
    // -----------------------------------------------------------------
    reg('rosetta.showReport', async (runId?: string) => {
        await vscode.window.withProgress(
            { location: vscode.ProgressLocation.Notification, title: 'Loading report...' },
            async () => {
                try {
                    const result = await cli.resultShow(runId);
                    if (!result.ok || !result.data) {
                        vscode.window.showErrorMessage(`Could not load report: ${result.error ?? 'No data'}`);
                        return;
                    }
                    const reportFiles: string[] = result.data.report_files ?? [];
                    const htmlReport = reportFiles.find(f => f.endsWith('.html'));
                    if (htmlReport && fs.existsSync(htmlReport)) {
                        showReportWebview(htmlReport, `Report: ${result.data.run_id ?? 'latest'}`);
                    } else {
                        vscode.window.showInformationMessage(`No HTML report found.`);
                    }
                } catch (e: any) {
                    vscode.window.showErrorMessage(`Report error: ${e.message}`);
                }
            },
        );
    });

    // -----------------------------------------------------------------
    // Install CLI / Server start+stop
    // -----------------------------------------------------------------
    reg('rosetta.installCLI', () => cli.install());

    reg('rosetta.startServer', async () => {
        try {
            await server.start();
            vscode.window.showInformationMessage(`Rosetta server started on port ${server.port}`);
        } catch (e: any) {
            vscode.window.showErrorMessage(`Server start failed: ${e.message}`);
        }
    });

    reg('rosetta.stopServer', () => {
        server.stop();
        vscode.window.showInformationMessage('Rosetta server stopped.');
    });

    // -----------------------------------------------------------------
    // Change config file path
    // -----------------------------------------------------------------
    reg('rosetta.changeConfig', async () => {
        const changed = await cli.changeConfig();
        if (changed) {
            dbmsTree.refresh();
        }
    });

    // -----------------------------------------------------------------
    // Delete history item
    // -----------------------------------------------------------------
    reg('rosetta.deleteHistoryItem', async (item: any) => {
        if (!item?.runDir) { return; }

        const name = item.label ?? path.basename(item.runDir);
        const confirm = await vscode.window.showWarningMessage(
            `Delete run "${name}" and all its reports?`,
            { modal: true },
            'Delete',
        );
        if (confirm !== 'Delete') { return; }

        try {
            fs.rmSync(item.runDir, { recursive: true, force: true });
            vscode.window.showInformationMessage(`Deleted: ${name}`);
            historyTree.refresh();
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to delete: ${e.message}`);
        }
    });
}
