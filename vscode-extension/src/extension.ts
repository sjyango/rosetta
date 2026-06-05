/**
 * Rosetta SQL — VS Code Extension
 */

import * as vscode from 'vscode';
import { RosettaCLI } from './rosettaCli';
import { RosettaServer } from './rosettaServer';
import { DbmsTreeProvider } from './views/dbmsTree';
import { HistoryTreeProvider } from './views/historyTree';
import { TestCodeLensProvider } from './providers/codeLens';
import { registerCommands } from './commands';

let cli: RosettaCLI;
let server: RosettaServer;
let dbmsTree: DbmsTreeProvider;
let historyTree: HistoryTreeProvider;
let outputChannel: vscode.OutputChannel;
let statusBarItem: vscode.StatusBarItem;

export async function activate(context: vscode.ExtensionContext) {
    outputChannel = vscode.window.createOutputChannel('Rosetta SQL');
    outputChannel.appendLine('Rosetta SQL extension activating...');

    // --- Core services ---------------------------------------------------
    cli = new RosettaCLI(outputChannel, context.globalState);
    server = new RosettaServer(cli, outputChannel);

    // --- Check CLI availability -----------------------------------------
    const available = await cli.isAvailable();
    if (!available) {
        const choice = await vscode.window.showWarningMessage(
            'Rosetta CLI not found. Install via pip?',
            'Install Now', 'Configure Path', 'Dismiss',
        );
        if (choice === 'Install Now') {
            await cli.install();
        } else if (choice === 'Configure Path') {
            vscode.commands.executeCommand('workbench.action.openSettings', 'rosetta.executable');
        }
    } else {
        const version = await cli.getVersion();
        outputChannel.appendLine(`Rosetta CLI found: ${version}`);
    }

    // --- Tree views ------------------------------------------------------
    dbmsTree = new DbmsTreeProvider(cli);
    historyTree = new HistoryTreeProvider(cli);

    // When DBMS tree finishes loading, auto-refresh history + status bar
    dbmsTree.onDidRefresh = () => {
        historyTree.refresh();
        const connected = dbmsTree.connectedDbms.length;
        statusBarItem.text = connected > 0
            ? `$(database) Rosetta ${connected} connected`
            : '$(warning) Rosetta';
    };

    context.subscriptions.push(
        vscode.window.registerTreeDataProvider('rosetta.dbmsView', dbmsTree),
        vscode.window.registerTreeDataProvider('rosetta.historyView', historyTree),
    );

    // --- CodeLens for .test files ----------------------------------------
    context.subscriptions.push(
        vscode.languages.registerCodeLensProvider(
            { pattern: '**/*.test' },
            new TestCodeLensProvider(),
        ),
    );

    // --- Status bar ------------------------------------------------------
    statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    statusBarItem.command = 'rosetta.refreshStatus';
    statusBarItem.text = '$(database) Rosetta';
    statusBarItem.tooltip = 'Rosetta SQL — Click to refresh';
    statusBarItem.show();
    context.subscriptions.push(statusBarItem);

    // --- Commands --------------------------------------------------------
    registerCommands(context, cli, server, dbmsTree, historyTree, outputChannel, statusBarItem);

    // --- Initial refresh (triggers onDidRefresh → history + status bar) --
    if (available) {
        dbmsTree.refresh();
    }

    outputChannel.appendLine('Rosetta SQL extension activated.');
}

export function deactivate() {
    server?.stop();
    outputChannel?.appendLine('Rosetta SQL extension deactivated.');
}
