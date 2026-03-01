# What hsqldb.txt would need to become a functional Claude skill

## 1. File location and format

Move to `.claude/commands/hsqldb.md` (or a name of your choice). Claude Code discovers
skills from that directory and exposes them as `/hsqldb`. The `.txt` extension and current
location make it invisible to the skill system. Convert plain text to Markdown — headers,
fenced code blocks — which Claude parses more reliably than ad-hoc ASCII formatting.

## 2. An imperative invocation wrapper at the top

The document is currently written as a *reference specification* — declarative and
encyclopaedic. A skill needs an unambiguous opening section that says "when invoked, do
these steps in this order." Something like:

> When this skill is invoked: (1) check for `generate.txt` in the project root and note
> any overrides; (2) read `model.xml`; (3) validate the model fully before writing any
> files; (4) generate...

Right now the workflow has to be inferred by reading the whole document; it isn't stated
up front.

## 3. Explicit file discovery

The spec assumes the model is available but never says *how to find it*. A skill should
say: "Look for `model.xml` in the project root. If it is absent, ask the user for the
path before proceeding." The same applies to `generate.txt` and `generated-inventory.txt`.

## 4. Argument support (`$ARGUMENTS`)

Claude Code passes the text after the command name as `$ARGUMENTS`. The skill should
declare whether it accepts an argument (e.g., an alternative model path) and what to do
if none is provided (default to `model.xml` in the project root).

## 5. Error surfacing protocol

The spec says "stop and raise the issue" for duplicate fields and reserved words, which is
good, but a skill should be explicit that this means *ask the user via a question before
writing any files*. The current wording is vague about the mechanism.

## 6. Remove or rephrase the "emit updated spec" final step

This step tells the generator to write a new version of itself (e.g., `hsqldb1.txt`). In
a skill context the skill file *is* the spec — the self-versioning concept doesn't
translate cleanly. The step should either be removed, or rephrased as "if clarifications
were needed during this run, note them as a comment to the user so the skill can be
updated manually."

## 7. `generated-inventory.txt` management made explicit in the workflow

The cleanup and inventory steps are described in a separate section near the end. In a
skill, these need to be woven into the main workflow sequence so they aren't skipped.
Currently a reader has to notice that section exists and connect it to the rest.

## 8. Progress communication

A well-designed skill tells the user what it is doing at each stage ("Validating
model… Generating beans… Running `mvn install`…"). The current spec is silent on this.

---

## Summary

The *content* of `hsqldb.txt` is already thorough and accurate — the domain rules,
patterns, and checklist are solid. The changes needed are almost entirely *structural and
contextual*: move it to the right place, wrap it in an explicit step-by-step workflow,
ground it in the filesystem (file discovery, arguments), clarify error-surfacing, and
remove the self-versioning step that doesn't fit the skill model.
