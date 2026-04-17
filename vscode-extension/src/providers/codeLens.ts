/**
 * CodeLens provider for .test files.
 *
 * Adds a clickable "▶ Run Test" lens above the first line of every .test file.
 */

import * as vscode from 'vscode';

export class TestCodeLensProvider implements vscode.CodeLensProvider {
    provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
        if (!document.fileName.endsWith('.test')) {
            return [];
        }

        const range = new vscode.Range(0, 0, 0, 0);

        return [
            new vscode.CodeLens(range, {
                title: '$(play) Run MTR Test',
                command: 'rosetta.runTest',
                arguments: [document.uri],
                tooltip: 'Execute this .test file across all configured DBMS',
            }),
            new vscode.CodeLens(range, {
                title: '$(open-preview) View Latest Report',
                command: 'rosetta.showReport',
                tooltip: 'Open the latest report for this test',
            }),
        ];
    }
}
