/**
 * Run History tree view provider.
 *
 * Stores the run directory path in each item so clicking a history entry
 * can directly open the HTML report without an extra CLI call.
 */

import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { RosettaCLI } from '../rosettaCli';

interface RunEntry {
    idx: number;
    id: string;
    type: string;
    dbms: string;
    timestamp: string;
}

export class HistoryTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    private _onDidChange = new vscode.EventEmitter<vscode.TreeItem | undefined>();
    readonly onDidChangeTreeData = this._onDidChange.event;

    private _items: vscode.TreeItem[] = [];
    private _cli: RosettaCLI;

    constructor(cli: RosettaCLI) {
        this._cli = cli;
    }

    refresh(): void {
        this._items = [new HistoryLoadingItem()];
        this._onDidChange.fire(undefined);
        this._fetch();
    }

    getTreeItem(element: vscode.TreeItem): vscode.TreeItem {
        return element;
    }

    getChildren(): vscode.TreeItem[] {
        return this._items;
    }

    private async _fetch(): Promise<void> {
        try {
            const result = await this._cli.resultList(50);
            if (!result.ok || !result.data?.runs) {
                this._items = [];
                this._onDidChange.fire(undefined);
                return;
            }

            // Resolve the results output directory so we can build direct file paths
            const outputDir = this._resolveOutputDir(result.data.output_dir);

            this._items = (result.data.runs as RunEntry[]).map(run => {
                return new HistoryItem(run, outputDir);
            });
        } catch {
            this._items = [];
        }

        this._onDidChange.fire(undefined);
    }

    private _resolveOutputDir(outputDir?: string): string {
        const dir = outputDir ?? 'results';
        if (path.isAbsolute(dir)) { return dir; }
        // Resolve relative to config file directory (same as CLI cwd)
        const configDir = path.dirname(this._cli.getConfigPath());
        return path.resolve(configDir, dir);
    }
}

class HistoryItem extends vscode.TreeItem {
    public readonly runDir: string;

    constructor(run: RunEntry, outputDir: string) {
        super(run.id, vscode.TreeItemCollapsibleState.None);

        this.runDir = path.join(outputDir, run.id);
        const isBench = run.type === 'bench';
        this.description = `${run.timestamp}`;
        this.iconPath = new vscode.ThemeIcon(
            isBench ? 'graph' : 'beaker',
            isBench
                ? new vscode.ThemeColor('charts.orange')
                : new vscode.ThemeColor('charts.green'),
        );
        this.tooltip = `${run.id}\nType: ${run.type}\nDBMS: ${run.dbms}\nTime: ${run.timestamp}\n\nClick to open report · Right-click to delete`;

        let htmlPath: string | undefined;
        try {
            if (fs.existsSync(this.runDir)) {
                const htmlFiles = fs.readdirSync(this.runDir).filter(f => f.endsWith('.html'));
                if (htmlFiles.length > 0) {
                    htmlPath = path.join(this.runDir, htmlFiles[0]);
                }
            }
        } catch { /* ignore */ }

        this.command = {
            command: 'rosetta.showReportFile',
            title: 'Show Report',
            arguments: [htmlPath, `${run.type}: ${run.id}`],
        };
        this.contextValue = 'historyItem';
    }
}

class HistoryLoadingItem extends vscode.TreeItem {
    constructor() {
        super('Loading history...', vscode.TreeItemCollapsibleState.None);
        this.iconPath = new vscode.ThemeIcon('sync~spin');
    }
}
