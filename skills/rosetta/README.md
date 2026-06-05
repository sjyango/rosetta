# Rosetta Skill

This Skill provides automated installation and usage guidance for rosetta, a cross-DBMS SQL testing and benchmarking toolkit.

## What's Included

```
skills/rosetta/
├── SKILL.md                    # Main Skill documentation
├── scripts/
│   ├── install_rosetta.py     # Automated installation script
│   └── rosetta_wrapper.py     # Command wrapper with convenience functions
├── references/
│   ├── commands.md             # Complete command reference
│   ├── config-guide.md         # Configuration guide
│   └── troubleshooting.md      # Troubleshooting guide
└── examples/
    ├── rosetta_config.example.json # Database configuration example
    └── bench.example.json       # Benchmark configuration example
```

## Quick Start

### For Skill Users

1. **Install rosetta:**
   ```bash
   python skills/rosetta/scripts/install_rosetta.py
   ```

2. **Generate configuration:**
   ```bash
   python skills/rosetta/scripts/rosetta_wrapper.py --setup-config
   ```

3. **Edit configuration with your database credentials:**
   ```bash
   vim rosetta_config.json
   ```

4. **Check database connections:**
   ```bash
   python skills/rosetta/scripts/rosetta_wrapper.py --check-connection
   ```

5. **Start using rosetta:**
   ```bash
   # Execute SQL
   python skills/rosetta/scripts/rosetta_wrapper.py exec --dbms mysql --sql "SELECT VERSION()"
   
   # Run MTR test
   python skills/rosetta/scripts/rosetta_wrapper.py mtr -t test.test --dbms mysql,tdsql
   
   # Run benchmark
   python skills/rosetta/scripts/rosetta_wrapper.py bench --file bench.json --dbms mysql,tdsql
   ```

### For Skill Developers

To distribute this Skill:

1. Ensure all files are in place:
   ```bash
   ls -R skills/rosetta/
   ```

2. Create a release or share the `skills/rosetta/` directory

3. Users can install by:
   - Cloning the repository
   - Or downloading the Skill directory
   - Then running `install_rosetta.py`

## Installation Methods

The Skill supports two installation methods:

### Method 1: From GitHub Release (Recommended)

- Downloads pre-compiled `.pyz` file
- Faster installation
- Version-locked

### Method 2: From Source

- Clones GitHub repository
- Installs via pip
- Latest development version

## GITHUB_TOKEN (Optional)

For higher API rate limits when downloading from GitHub:

```bash
# Set environment variable
export GITHUB_TOKEN="your_personal_access_token"

# Or add to shell config
echo 'export GITHUB_TOKEN="your_token"' >> ~/.bashrc
```

Create token at: https://github.com/settings/tokens

## Requirements

- Python >= 3.8
- Network connection (for installation)
- Database access credentials

## Dependencies

Automatically installed by the installation script:
- pymysql >= 1.0
- rich >= 13.0
- prompt_toolkit >= 3.0

## Documentation

- **SKILL.md**: Main documentation with usage examples
- **references/commands.md**: Complete command reference
- **references/config-guide.md**: Configuration file guide
- **references/troubleshooting.md**: Common issues and solutions

## Examples

- **examples/rosetta_config.example.json**: Multi-database configuration
- **examples/bench.example.json**: Benchmark test definition

## Support

- GitHub: https://github.com/sjyango/rosetta
- Issues: https://github.com/sjyango/rosetta/issues

## License

See LICENSE file in the rosetta repository.
