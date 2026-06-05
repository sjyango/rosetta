/**
 * Rosetta CLI wrapper — discovers and invokes the `rosetta` command.
 *
 * All commands use `-j` (JSON mode) so output is machine-parseable.
 * The CLI is installed via `pip install rosetta-sql`.
 */

import * as vscode from 'vscode';
import * as fs from 'fs';
import { execFile, spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as os from 'os';

/** Structured result from `rosetta -j <command>`. */
export interface CommandResult {
    ok: boolean;
    command: string;
    timestamp: string;
    data?: Record<string, any>;
    error?: string;
}

export class RosettaCLI {
    private _executablePath: string | null = null;
    private _configPathOverride: string | null = null;
    private _outputChannel: vscode.OutputChannel;
    private _globalState: vscode.Memento | null = null;

    private static readonly CONFIG_STATE_KEY = 'rosetta.lastConfigPath';

    constructor(outputChannel: vscode.OutputChannel, globalState?: vscode.Memento) {
        this._outputChannel = outputChannel;
        this._globalState = globalState ?? null;

        // Restore persisted config path from last session
        if (this._globalState) {
            const saved = this._globalState.get<string>(RosettaCLI.CONFIG_STATE_KEY);
            if (saved && fs.existsSync(saved)) {
                this._configPathOverride = saved;
                this._outputChannel.appendLine(`Restored config path: ${saved}`);
            }
        }
    }

    // -----------------------------------------------------------------
    // Path resolution
    // -----------------------------------------------------------------

    /** Resolve the rosetta executable path. */
    async getExecutablePath(): Promise<string> {
        if (this._executablePath) {
            return this._executablePath;
        }

        const config = vscode.workspace.getConfiguration('rosetta');
        const configured = config.get<string>('executable');
        if (configured) {
            this._executablePath = configured;
            return configured;
        }

        // Try to find in PATH
        const found = await this._which('rosetta');
        if (found) {
            this._executablePath = found;
            return found;
        }

        // Try python -m rosetta
        this._executablePath = 'rosetta';
        return 'rosetta';
    }

    /** Check if rosetta CLI is available. */
    async isAvailable(): Promise<boolean> {
        try {
            const result = await this.exec(['--version', '-j']);
            return result.ok;
        } catch {
            return false;
        }
    }

    /** Get rosetta version string. */
    async getVersion(): Promise<string> {
        try {
            const result = await this.exec(['--version', '-j']);
            return result.data?.version ?? 'unknown';
        } catch {
            return 'unknown';
        }
    }

    /** Install rosetta via pip. */
    async install(): Promise<boolean> {
        const config = vscode.workspace.getConfiguration('rosetta');
        const python = config.get<string>('pythonPath') ?? 'python3';

        const terminal = vscode.window.createTerminal('Rosetta Install');
        terminal.show();
        terminal.sendText(`${python} -m pip install rosetta-sql`);

        // Wait and re-check
        const result = await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: 'Installing Rosetta CLI...',
                cancellable: false,
            },
            async () => {
                // Give pip some time
                await new Promise(r => setTimeout(r, 15000));
                this._executablePath = null;
                return this.isAvailable();
            },
        );

        if (result) {
            vscode.window.showInformationMessage('Rosetta CLI installed successfully!');
        } else {
            vscode.window.showErrorMessage(
                'Rosetta CLI installation may not be complete. Check the terminal output.',
            );
        }
        return result;
    }

    // -----------------------------------------------------------------
    // Config helpers
    // -----------------------------------------------------------------

    /** Get the resolved config file path (synchronous, from settings). */
    getConfigPath(): string {
        // Use override if set (avoids race with async settings update)
        if (this._configPathOverride) {
            return this._configPathOverride;
        }
        const config = vscode.workspace.getConfiguration('rosetta');
        const configPath = config.get<string>('configPath') ?? path.join(os.homedir(), '.rosetta', 'config.json');
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        if (workspaceRoot && !path.isAbsolute(configPath)) {
            return path.join(workspaceRoot, configPath);
        }
        return configPath;
    }

    /** Check if the config file exists. */
    configExists(): boolean {
        return fs.existsSync(this.getConfigPath());
    }

    /**
     * Let the user change the config file path at any time.
     * Returns true if a new path was set.
     */
    async changeConfig(): Promise<boolean> {
        const currentPath = this.getConfigPath();
        const choice = await vscode.window.showQuickPick(
            [
                { label: '$(folder-opened) Browse File', id: 'browse' },
                { label: '$(edit) Enter Path', id: 'enter', description: currentPath },
                { label: '$(add) Create New', id: 'create' },
            ],
            { title: 'Change Rosetta Config', placeHolder: `Current: ${currentPath}` },
        );

        if (!choice) { return false; }

        if (choice.id === 'browse') {
            const uris = await vscode.window.showOpenDialog({
                canSelectMany: false,
                filters: { 'JSON Config': ['json'] },
                title: 'Select Rosetta DBMS config file',
                openLabel: 'Use this config',
                defaultUri: fs.existsSync(currentPath) ? vscode.Uri.file(path.dirname(currentPath)) : undefined,
            });
            if (uris && uris.length > 0) {
                await this._saveConfigPath(uris[0].fsPath);
                return true;
            }
        } else if (choice.id === 'enter') {
            const input = await vscode.window.showInputBox({
                prompt: 'Enter the path to your config.json',
                placeHolder: '/path/to/config.json',
                value: currentPath,
                validateInput: (val) => {
                    if (!val.trim()) { return 'Path cannot be empty'; }
                    const resolved = this._resolvePath(val.trim());
                    if (!fs.existsSync(resolved)) { return `File not found: ${resolved}`; }
                    return undefined;
                },
            });
            if (input) {
                await this._saveConfigPath(this._resolvePath(input.trim()));
                return true;
            }
        } else if (choice.id === 'create') {
            return this._createNewConfig();
        }

        return false;
    }

    /**
     * Ensure a valid config file is available.
     *
     * If the current config path does not exist, the user is prompted to
     * either browse for an existing file, create a new one via
     * `rosetta config init`, or enter a path manually.
     *
     * Returns `true` if a valid config is now set, `false` if the user
     * cancelled.
     */
    async ensureConfig(): Promise<boolean> {
        if (this.configExists()) {
            return true;
        }

        const currentPath = this.getConfigPath();
        const choice = await vscode.window.showWarningMessage(
            `Config file not found: ${currentPath}`,
            'Browse File',
            'Create New',
            'Enter Path',
        );

        if (choice === 'Browse File') {
            const uris = await vscode.window.showOpenDialog({
                canSelectMany: false,
                filters: { 'JSON Config': ['json'] },
                title: 'Select Rosetta DBMS config file',
                openLabel: 'Use this config',
            });
            if (uris && uris.length > 0) {
                await this._saveConfigPath(uris[0].fsPath);
                return true;
            }
        } else if (choice === 'Create New') {
            return this._createNewConfig();
        } else if (choice === 'Enter Path') {
            const input = await vscode.window.showInputBox({
                prompt: 'Enter the path to your config.json',
                placeHolder: '/path/to/config.json',
                value: currentPath,
                validateInput: (val) => {
                    if (!val.trim()) { return 'Path cannot be empty'; }
                    const resolved = this._resolvePath(val.trim());
                    if (!fs.existsSync(resolved)) {
                        return `File not found: ${resolved}`;
                    }
                    return undefined;
                },
            });
            if (input) {
                const resolved = this._resolvePath(input.trim());
                await this._saveConfigPath(resolved);
                vscode.window.showInformationMessage(`Config set to: ${resolved}`);
                return true;
            }
        }

        return false;
    }

    /** Save the config path — in-memory + globalState + best-effort settings. */
    private async _saveConfigPath(absolutePath: string): Promise<void> {
        // 1. Immediately cache for this session
        this._configPathOverride = absolutePath;
        this._outputChannel.appendLine(`Config path set: ${absolutePath}`);

        // 2. Persist to globalState (survives IDE restarts, no workspace needed)
        if (this._globalState) {
            await this._globalState.update(RosettaCLI.CONFIG_STATE_KEY, absolutePath);
        }

        // 3. Best-effort persist to VS Code settings
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        let savePath = absolutePath;
        if (workspaceRoot && absolutePath.startsWith(workspaceRoot + path.sep)) {
            savePath = path.relative(workspaceRoot, absolutePath);
        }

        const config = vscode.workspace.getConfiguration('rosetta');
        try {
            await config.update('configPath', savePath, vscode.ConfigurationTarget.Workspace);
        } catch {
            try {
                await config.update('configPath', absolutePath, vscode.ConfigurationTarget.Global);
            } catch {
                // globalState already has it — this session and future sessions are fine
            }
        }
    }

    /** Create a new config via `rosetta config init`. */
    private async _createNewConfig(): Promise<boolean> {
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        if (!workspaceRoot) {
            vscode.window.showErrorMessage('No workspace folder open.');
            return false;
        }

        try {
            const exe = await this.getExecutablePath();
            const outputPath = path.join(os.homedir(), '.rosetta', 'config.json');

            await new Promise<void>((resolve, reject) => {
                execFile(exe, ['config', 'init', '--output', outputPath], { cwd: workspaceRoot }, (err) => {
                    if (err) { reject(err); } else { resolve(); }
                });
            });

            await this._saveConfigPath(outputPath);

            // Open the file for editing
            const doc = await vscode.workspace.openTextDocument(outputPath);
            await vscode.window.showTextDocument(doc);
            vscode.window.showInformationMessage(
                'Created config.json — edit the database connections, then refresh.',
            );
            return true;
        } catch (e: any) {
            vscode.window.showErrorMessage(`Failed to create config: ${e.message}`);
            return false;
        }
    }

    /** Resolve a potentially relative path against workspace root. */
    private _resolvePath(input: string): string {
        // Expand ~ to home directory
        if (input.startsWith('~')) {
            const home = process.env.HOME ?? process.env.USERPROFILE ?? '';
            input = path.join(home, input.slice(1));
        }
        if (path.isAbsolute(input)) { return input; }
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        return workspaceRoot ? path.resolve(workspaceRoot, input) : path.resolve(input);
    }

    // -----------------------------------------------------------------
    // Core execution
    // -----------------------------------------------------------------

    /** Execute a rosetta CLI command and return parsed JSON result. */
    async exec(args: string[], cwd?: string): Promise<CommandResult> {
        const exe = await this.getExecutablePath();
        const configPath = this.getConfigPath();

        // Always use JSON mode; inject -c if not already in args
        const fullArgs = ['-j'];
        if (!args.includes('-c') && !args.includes('--config')) {
            fullArgs.push('-c', configPath);
        }
        fullArgs.push(...args);

        // Default cwd: directory containing the config file (so relative
        // paths like "results" resolve correctly), fallback to workspace root.
        const workDir = cwd
            ?? path.dirname(configPath)
            ?? vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        this._outputChannel.appendLine(`> rosetta ${fullArgs.join(' ')}`);

        return new Promise<CommandResult>((resolve, reject) => {
            execFile(exe, fullArgs, { cwd: workDir, maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
                if (stderr) {
                    this._outputChannel.appendLine(stderr);
                }
                if (err && !stdout) {
                    reject(new Error(err.message));
                    return;
                }
                try {
                    const result: CommandResult = JSON.parse(stdout);
                    resolve(result);
                } catch {
                    reject(new Error(`Failed to parse JSON output: ${stdout.slice(0, 200)}`));
                }
            });
        });
    }

    /**
     * Execute a rosetta CLI command with real-time progress tracking.
     *
     * Spawns the process and parses stderr for progress indicators
     * (Rich progress bars output patterns like "N/M" or percentage).
     * Calls `onProgress(percent, message)` as progress is detected.
     */
    async execWithProgress(
        args: string[],
        onProgress: (percent: number, message: string) => void,
    ): Promise<CommandResult> {
        const exe = await this.getExecutablePath();
        const configPath = this.getConfigPath();

        const fullArgs = ['-j'];
        if (!args.includes('-c') && !args.includes('--config')) {
            fullArgs.push('-c', configPath);
        }
        fullArgs.push(...args);

        const workDir = path.dirname(configPath)
            ?? vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        this._outputChannel.appendLine(`> rosetta ${fullArgs.join(' ')} (progress)`);

        return new Promise<CommandResult>((resolve, reject) => {
            const proc = spawn(exe, fullArgs, {
                cwd: workDir,
                stdio: ['pipe', 'pipe', 'pipe'],
            });

            let stdout = '';
            let stderr = '';
            let lastPercent = 0;

            proc.stdout?.on('data', (data: Buffer) => {
                stdout += data.toString();
            });

            proc.stderr?.on('data', (data: Buffer) => {
                const text = data.toString();
                stderr += text;
                this._outputChannel.appendLine(text.trimEnd());

                // Parse progress from Rich output — look for "N/M" patterns
                // Rich progress bar outputs: "  mysql  23/45  |  0:05  |  ..."
                const matches = text.match(/(\d+)\/(\d+)/g);
                if (matches) {
                    for (const m of matches) {
                        const [cur, total] = m.split('/').map(Number);
                        if (total > 0 && cur <= total) {
                            const pct = Math.round((cur / total) * 100);
                            if (pct > lastPercent) {
                                lastPercent = pct;
                                onProgress(pct, `${cur}/${total} statements`);
                            }
                        }
                    }
                }

                // Also detect phase keywords
                const lower = text.toLowerCase();
                if (lower.includes('parse')) {
                    onProgress(lastPercent, 'Parsing test file...');
                } else if (lower.includes('connect')) {
                    onProgress(lastPercent, 'Connecting to databases...');
                } else if (lower.includes('compar')) {
                    onProgress(Math.max(lastPercent, 80), 'Comparing results...');
                } else if (lower.includes('report')) {
                    onProgress(Math.max(lastPercent, 90), 'Generating reports...');
                }
            });

            proc.on('close', (code) => {
                onProgress(100, 'Complete');
                if (!stdout.trim()) {
                    reject(new Error(`Process exited with code ${code}\n${stderr.slice(-500)}`));
                    return;
                }
                try {
                    resolve(JSON.parse(stdout));
                } catch {
                    reject(new Error(`Failed to parse output: ${stdout.slice(0, 300)}`));
                }
            });

            proc.on('error', (e) => reject(e));
        });
    }

    /** Spawn a long-running rosetta process (e.g. interactive server). */
    spawn(args: string[], cwd?: string): ChildProcess {
        const exe = this._executablePath ?? 'rosetta';
        const configPath = this.getConfigPath();
        const fullArgs = ['-c', configPath, ...args];
        const workDir = cwd
            ?? path.dirname(configPath)
            ?? vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;

        this._outputChannel.appendLine(`> rosetta ${fullArgs.join(' ')} (spawn)`);

        return spawn(exe, fullArgs, {
            cwd: workDir,
            stdio: ['pipe', 'pipe', 'pipe'],
        });
    }

    // -----------------------------------------------------------------
    // Shortcut methods
    // -----------------------------------------------------------------

    /** Check DBMS connection status. */
    async status(): Promise<CommandResult> {
        return this.exec(['status']);
    }

    /** Run an MTR test. */
    async mtr(testFile: string, dbms?: string, baseline?: string): Promise<CommandResult> {
        const args = ['mtr', '-t', testFile];
        if (dbms) { args.push('--dbms', dbms); }
        if (baseline) { args.push('--baseline', baseline); }
        return this.exec(args);
    }

    /** Run a benchmark. */
    async bench(benchFile: string, dbms?: string): Promise<CommandResult> {
        const args = ['bench', '--file', benchFile];
        if (dbms) { args.push('--dbms', dbms); }
        return this.exec(args);
    }

    /** Execute SQL across databases. */
    async execSQL(sql: string, dbms?: string, database?: string): Promise<CommandResult> {
        const args = ['exec', '--sql', sql];
        if (dbms) { args.push('--dbms', dbms); }
        if (database) { args.push('-d', database); }
        return this.exec(args);
    }

    /** List run history. */
    async resultList(limit = 20, page = 1): Promise<CommandResult> {
        return this.exec(['result', 'list', '-n', String(limit), '-p', String(page)]);
    }

    /** Show details of a specific run. */
    async resultShow(runId?: string): Promise<CommandResult> {
        const args = ['result', 'show'];
        if (runId) { args.push(runId); }
        return this.exec(args);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    private _which(command: string): Promise<string | null> {
        return new Promise(resolve => {
            const cmd = process.platform === 'win32' ? 'where' : 'which';
            execFile(cmd, [command], (err, stdout) => {
                if (err || !stdout.trim()) {
                    resolve(null);
                } else {
                    resolve(stdout.trim().split('\n')[0]);
                }
            });
        });
    }
}
