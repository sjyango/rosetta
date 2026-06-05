"""Variable store for MTR variable system.

Handles $var_name and ${var_name} references, variable assignment,
increment/decrement, expression evaluation, and variable substitution
in strings (the do_eval equivalent from mysqltest.cc).
"""

from __future__ import annotations

import re
from typing import Dict, Optional


class VariableError(Exception):
    """Error in variable operations."""
    pass


class VariableStore:
    """Store and manage MTR variables.

    Supports:
      - Simple assignment: --let $var = value
      - Variable references: $var, ${var}
      - Escaped dollar: \\$var -> literal $var
      - Increment/decrement: --inc $var, --dec $var
      - Expression: --expr $var = $op1 <operator> $op2
      - Query result assignment: --let $var = `query`
    """

    _VAR_PATTERN = re.compile(
        r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}'   # ${var_name}
        r'|\$([a-zA-Z_][a-zA-Z0-9_]*)'       # $var_name
    )

    # Matches a backtick-quoted SQL query for variable assignment
    _QUERY_PATTERN = re.compile(r'^`(.*)`$')

    def __init__(self, env_vars: Optional[Dict[str, str]] = None):
        self._vars: Dict[str, str] = {}
        self._int_vars: Dict[str, bool] = {}  # Track which vars are integers

        # Initialize with environment variables
        if env_vars:
            for k, v in env_vars.items():
                self.set(k, v)

    def get(self, name: str) -> str:
        """Get a variable's string value.

        Args:
            name: Variable name without the $ prefix.

        Returns:
            The variable's string value.

        Raises:
            VariableError: If the variable doesn't exist.
        """
        key = name.lstrip('$')
        if key not in self._vars:
            raise VariableError(f"Undefined variable: ${key}")
        return self._vars[key]

    def get_int(self, name: str) -> int:
        """Get a variable's integer value.

        Args:
            name: Variable name without the $ prefix.

        Returns:
            The variable's integer value.

        Raises:
            VariableError: If the variable is not an integer.
        """
        key = name.lstrip('$')
        if key not in self._vars:
            raise VariableError(f"Undefined variable: ${key}")
        if not self._int_vars.get(key):
            try:
                return int(self._vars[key])
            except (ValueError, TypeError):
                raise VariableError(
                    f"Variable ${key} is not an integer: {self._vars[key]!r}")
        return int(self._vars[key])

    def set(self, name: str, value: str) -> None:
        """Set a variable's value.

        Args:
            name: Variable name, may include $ prefix.
            value: The string value to assign.
        """
        key = name.lstrip('$').strip()
        str_val = str(value)
        self._vars[key] = str_val

        # Track if it's an integer
        try:
            int(str_val)
            self._int_vars[key] = True
        except (ValueError, TypeError):
            self._int_vars[key] = False

    def set_int(self, name: str, value: int) -> None:
        """Set a variable's integer value."""
        key = name.lstrip('$').strip()
        self._vars[key] = str(value)
        self._int_vars[key] = True

    def exists(self, name: str) -> bool:
        """Check if a variable exists."""
        key = name.lstrip('$')
        return key in self._vars

    def inc(self, name: str) -> None:
        """Increment an integer variable by 1."""
        key = name.lstrip('$')
        if key not in self._vars:
            raise VariableError(f"Cannot inc undefined variable: ${key}")
        if not self._int_vars.get(key):
            raise VariableError(
                f"Cannot inc non-numeric variable: ${key}={self._vars[key]!r}")
        val = int(self._vars[key]) + 1
        self._vars[key] = str(val)

    def dec(self, name: str) -> None:
        """Decrement an integer variable by 1."""
        key = name.lstrip('$')
        if key not in self._vars:
            raise VariableError(f"Cannot dec undefined variable: ${key}")
        if not self._int_vars.get(key):
            raise VariableError(
                f"Cannot dec non-numeric variable: ${key}={self._vars[key]!r}")
        val = int(self._vars[key]) - 1
        self._vars[key] = str(val)

    def evaluate_expr(self, op1: str, operator: str, op2: str) -> int:
        """Evaluate a math expression: op1 <operator> op2.

        Operators: +, -, *, /, %

        Args:
            op1: First operand (variable name with $ or literal number).
            operator: The arithmetic operator.
            op2: Second operand (variable name with $ or literal number).

        Returns:
            The result as an integer.
        """
        v1 = self._resolve_numeric(op1)
        v2 = self._resolve_numeric(op2)

        if operator == '+':
            result = v1 + v2
        elif operator == '-':
            result = v1 - v2
        elif operator == '*':
            result = v1 * v2
        elif operator == '/':
            if v2 == 0:
                raise VariableError("Division by zero")
            result = v1 // v2
        elif operator == '%':
            if v2 == 0:
                raise VariableError("Modulo by zero")
            result = v1 % v2
        else:
            raise VariableError(f"Unknown operator: {operator}")

        return result

    def _resolve_numeric(self, operand: str) -> int:
        """Resolve an operand to an integer value.

        If it starts with $, it's a variable reference.
        Otherwise, it's a literal number.
        """
        operand = operand.strip()
        if operand.startswith('$'):
            return self.get_int(operand)
        try:
            return int(operand)
        except ValueError:
            raise VariableError(
                f"Non-numeric operand (not a variable): {operand!r}")

    def evaluate_condition(self, var_name: str, negated: bool = False,
                           operator: Optional[str] = None,
                           right_operand: Optional[str] = None) -> bool:
        """Evaluate a condition expression for if/while/assert.

        Supports:
          - $var (truthy check: non-zero int or non-empty string)
          - !$var (negated truthy check)
          - $var == value, $var != value (string or int comparison)
          - $var < N, $var <= N, $var > N, $var >= N (int comparison only)

        Args:
            var_name: The variable name (with $).
            negated: Whether the condition is negated with !.
            operator: Comparison operator, if any.
            right_operand: Right operand for comparison, if any.

        Returns:
            Boolean result of the condition.
        """
        key = var_name.lstrip('$')
        if key not in self._vars:
            val_str = ""
            is_int = False
            int_val = 0
        else:
            val_str = self._vars[key]
            is_int = self._int_vars.get(key, False)
            try:
                int_val = int(val_str)
            except (ValueError, TypeError):
                is_int = False
                int_val = 0

        if operator is None:
            # Simple truthy check
            if is_int:
                result = int_val != 0
            else:
                # Non-empty string not starting with 0 is truthy
                stripped = val_str.strip().lstrip('+-')
                result = bool(stripped) and stripped[0] != '0'
        else:
            # Comparison
            right_val = right_operand.strip().strip("'\"")
            if operator in ('==', '!='):
                # Support both string and int comparison
                if is_int:
                    try:
                        right_int = int(right_val)
                        if operator == '==':
                            result = int_val == right_int
                        else:
                            result = int_val != right_int
                    except ValueError:
                        # String comparison fallback
                        if operator == '==':
                            result = val_str == right_val
                        else:
                            result = val_str != right_val
                else:
                    if operator == '==':
                        result = val_str == right_val
                    else:
                        result = val_str != right_val
            elif operator in ('<', '<=', '>', '>='):
                if not is_int:
                    raise VariableError(
                        f"Only == and != are supported for string values, "
                        f"got {operator} for ${key}={val_str!r}")
                try:
                    right_int = int(right_val)
                except ValueError:
                    raise VariableError(
                        f"Right operand must be numeric for {operator}: "
                        f"{right_val!r}")
                if operator == '<':
                    result = int_val < right_int
                elif operator == '<=':
                    result = int_val <= right_int
                elif operator == '>':
                    result = int_val > right_int
                else:  # >=
                    result = int_val >= right_int
            else:
                raise VariableError(f"Unknown comparison operator: {operator}")

        if negated:
            result = not result

        return result

    def substitute(self, text: str, pass_through_escape: bool = False) -> str:
        """Substitute variable references in a string.

        This is the Python equivalent of do_eval() in mysqltest.cc.

        Supports:
          - $var_name -> variable value
          - ${var_name} -> variable value
          - \\$var_name -> literal $var_name (if pass_through_escape is True)

        Args:
            text: The text containing variable references.
            pass_through_escape: If True, \\$ is kept as literal $.

        Returns:
            The text with all variable references substituted.
        """
        result = []
        i = 0
        while i < len(text):
            if text[i] == '\\' and pass_through_escape and i + 1 < len(text):
                if text[i + 1] == '$':
                    # Escaped dollar - keep as literal $
                    result.append('$')
                    i += 2
                    continue
                else:
                    result.append(text[i])
                    i += 1
                    continue

            if text[i] == '$':
                # Try to match a variable reference
                var_match = re.match(
                    r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}'
                    r'|\$([a-zA-Z_][a-zA-Z0-9_]*)',
                    text[i:])
                if var_match:
                    name = var_match.group(1) or var_match.group(2)
                    if name in self._vars:
                        result.append(self._vars[name])
                    else:
                        # Undefined variable - leave as-is
                        result.append(text[i:i + var_match.end()])
                    i += var_match.end()
                    continue

            result.append(text[i])
            i += 1

        return ''.join(result)

    def to_dict(self) -> Dict[str, str]:
        """Return a copy of all variables as a dict."""
        return dict(self._vars)
