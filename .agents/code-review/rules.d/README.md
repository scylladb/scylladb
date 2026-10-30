# Code review rules

Project-specific review rules, one per file, shared by every review tool:
the code-review skill, CodeRabbit, and any other review bot we try. A rule is
written once here, and each tool's configuration includes this directory
instead of carrying its own copy.

## Rule files

- One rule per Markdown file, named after what it checks, for example
  `db-config-new-options.md`.
- The file starts with a `# Title` and an `Applies to:` line, listing the
  paths the rule is about. A tool that cannot scope a rule to paths includes
  every file, and relies on this line to skip the rules that don't apply.
- The rule says what to flag, and what not to flag, in tool-neutral terms.
  Tool-specific behavior, such as where to post a comment or how the rule
  interacts with the tool's other settings, belongs in that tool's
  configuration.

## Tools that include these rules

- CodeRabbit: `knowledge_base.code_guidelines.filePatterns` in
  `.coderabbit.yaml`.
