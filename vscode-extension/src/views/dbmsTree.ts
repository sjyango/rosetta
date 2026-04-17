/**
 * DBMS Connections tree view provider.
 */

import * as vscode from 'vscode';
import { RosettaCLI } from '../rosettaCli';

export interface DbmsStatus {
    name: string;
    host: string;
    port: number;
    driver: string;
    connected: boolean;
    port_reachable: boolean;
    version?: string;
    latency_ms?: number;
    error?: string;
}

export class DbmsTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    private _onDidChange = new vscode.EventEmitter<vscode.TreeItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChange.event;

    private _items: vscode.TreeItem[] = [];
    private _cli: RosettaCLI;
    private _cachedDbms: DbmsStatus[] = [];

    /** Optional callback invoked after a successful status fetch. */
    onDidRefresh?: () => void;

    constructor(cli: RosettaCLI) {
        this._cli = cli;
    }

    get connectedDbms(): DbmsStatus[] {
        return this._cachedDbms.filter(d => d.connected);
    }

    refresh(): void {
        this._items = [new LoadingItem()];
        this._onDidChange.fire(undefined);
        this._fetchStatus().catch(e => {
            this._items = [new ErrorItem(String(e))];
            this._onDidChange.fire(undefined);
        });
    }

    getTreeItem(element: vscode.TreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(): vscode.TreeItem[] {
        return this._items;
    }

    private async _fetchStatus(): Promise<void> {
        // --- Config check with interactive setup ---
        if (!this._cli.configExists()) {
            // Show the "no config" placeholder first
            this._items = [new ConfigMissingItem()];
            this._onDidChange.fire(undefined);

            const ok = await this._cli.ensureConfig();
            if (!ok) {
                // User cancelled — keep the placeholder
                return;
            }

            // Config was just set — show loading and continue
            this._items = [new LoadingItem()];
            this._onDidChange.fire(undefined);
        }

        // --- Fetch DBMS status ---
        try {
            const result = await this._cli.status();
            if (!result.ok || !result.data?.dbms) {
                this._cachedDbms = [];
                this._items = [new ErrorItem(result.error ?? 'Unknown error')];
                this._onDidChange.fire(undefined);
                return;
            }

            this._cachedDbms = result.data.dbms as DbmsStatus[];
            this._items = this._cachedDbms.map(db => {
                const desc = db.connected
                    ? `${db.version ?? ''} (${db.latency_ms?.toFixed(1) ?? '?'}ms)`
                    : db.error ?? 'Disconnected';
                return new DbmsItem(db.name, desc, db.connected, db.host, db.port, db.driver);
            });
        } catch (e: any) {
            this._cachedDbms = [];
            this._items = [new ErrorItem(e.message)];
        }

        this._onDidChange.fire(undefined);

        // Notify listeners (e.g. to refresh history + status bar)
        this.onDidRefresh?.();
    }
}

class DbmsItem extends vscode.TreeItem {
    constructor(
        public readonly name: string, public readonly status: string,
        public readonly connected: boolean,
        public readonly host?: string, public readonly port?: number,
        public readonly driver?: string,
    ) {
        super(name, vscode.TreeItemCollapsibleState.None);
        this.description = status;
        this.tooltip = host ? `${name}\n${host}:${port} (${driver})\n${status}` : status;
        this.iconPath = new vscode.ThemeIcon(
            connected ? 'database' : 'debug-disconnect',
            connected ? new vscode.ThemeColor('testing.iconPassed') : new vscode.ThemeColor('testing.iconFailed'),
        );
        this.contextValue = connected ? 'dbms-connected' : 'dbms-disconnected';
    }
}

class LoadingItem extends vscode.TreeItem {
    constructor() {
        super('Connecting...', vscode.TreeItemCollapsibleState.None);
        this.description = 'checking DBMS status';
        this.iconPath = new vscode.ThemeIcon('sync~spin');
    }
}

class ErrorItem extends vscode.TreeItem {
    constructor(msg: string) {
        super('Error', vscode.TreeItemCollapsibleState.None);
        this.description = msg;
        this.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
        this.command = { command: 'rosetta.refreshStatus', title: 'Retry' };
    }
}

class ConfigMissingItem extends vscode.TreeItem {
    constructor() {
        super('No config file', vscode.TreeItemCollapsibleState.None);
        this.description = 'Click to set up';
        this.tooltip = 'Click to browse, create, or enter a config file path.';
        this.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('problemsWarningIcon.foreground'));
        this.command = { command: 'rosetta.refreshStatus', title: 'Set up config' };
    }
}
