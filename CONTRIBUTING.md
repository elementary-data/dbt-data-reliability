# Contributing guidelines

_Note_: This document is for the dbt package.
For the Python package (`edr`), refer to
the [Python package contributing guidelines](https://github.com/elementary-data/elementary/blob/master/CONTRIBUTING.md).

## Getting started with development

### Setup

#### (1) Clone the repository

```
git clone https://github.com/elementary-data/dbt-data-reliability.git
cd dbt-data-reliability
```

#### (2) Edit `packages.yml` in your dbt project

```yaml
packages:
  - local: /path/to/dbt-data-reliability
```

#### (3) Install the package

```
dbt deps
```

You're done. Running `dbt` will now run the code in your local repository.

## First time contributors

If you're looking for things to help with, browse
our [issue tracker](https://github.com/elementary-data/elementary/issues)!

In particular, look for:

- [Open to contribution issues](https://github.com/elementary-data/elementary/labels/Open%20to%20contribution%20%F0%9F%A7%A1)
- [good first issues](https://github.com/elementary-data/elementary/labels/Good%20first%20issue%20%F0%9F%A5%87)
- [documentation issues](https://github.com/elementary-data/elementary/labels/documentation)

You do not need to ask for permission to work on any of these issues.
Just fix the issue yourself and [open a pull request](#submitting-changes).

To get help fixing a specific issue, it's often best to comment on the issue
itself. You're much more likely to get help if you provide details about what
you've tried and where you've
looked. [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) can also
be a good place
to ask for help.

## Submitting changes

Even more excellent than a good bug report is a fix for a bug, or the
implementation of a much-needed new feature. We'd love to have
your contributions.

We use the usual GitHub pull-request flow, which may be familiar to
you if you've contributed to other projects on GitHub. For the mechanics,
view [this guide](https://help.github.com/articles/using-pull-requests/).

If your change will be a significant amount of work
to write, we highly recommend starting by opening an issue laying out
what you want to do. That lets a conversation happen early in case
other contributors disagree with what you'd like to do or have ideas
that will help you do it.

The best pull requests are focused, clearly describe what they're for
and why they're correct, and contain tests for whatever changes they
make to the code's behavior. As a bonus these are easiest for someone
to review, which helps your pull request get merged quickly!
