# Materialize SQL parser

This parser is a fork of <https://github.com/andygrove/sqlparser-rs>, with
some additional patches from <http://github.com/nickolay/sqlparser-rs>.

At the time the parser was originally forked (March 2019), there was no modular
means by which its SQL syntax could be extended. This is particularly
unfortunate because the parser is under active upstream development, and we'd
like to periodically incorporate as much of their work as possible.

When hacking on this repository, please try to minimize the divergence from
upstream, e.g. by limiting unnecessary refactoring, so that upstream patches can
be easily incorporated.

At some point, if the parsers diverge enough, it may be worth jettisoning
compatibility with upstream so that we can perform large-scale refactors, but we
should make such a decision deliberately, not accidentally.

## Upstream overview

The goal of this project is to build a SQL lexer and parser capable of parsing
SQL that conforms with the [ANSI/ISO SQL standard][sql-standard] while also
making it easy to support custom dialects so that this crate can be used as a
foundation for vendor-specific parsers.

This parser is currently being used by the [DataFusion] query engine and
[LocustDB].

## Example

To parse a simple `SELECT` statement:

```rust
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

println!("AST: {:?}", ast);
```

This outputs

```rust
AST: [Query(Query { ctes: [], body: Select(Select { distinct: false, projection: [UnnamedExpr(Identifier("a")), UnnamedExpr(Identifier("b")), UnnamedExpr(Value(Long(123))), UnnamedExpr(Function(Function { name: ObjectName(["myfunc"]), args: [Identifier("b")], over: None, distinct: false }))], from: [TableWithJoins { relation: Table { name: ObjectName(["table_1"]), alias: None, args: [], with_hints: [] }, joins: [] }], selection: Some(BinaryOp { left: BinaryOp { left: Identifier("a"), op: Gt, right: Identifier("b") }, op: And, right: BinaryOp { left: Identifier("b"), op: Lt, right: Value(Long(100)) } }), group_by: [], having: None }), order_by: [OrderByExpr { expr: Identifier("a"), asc: Some(false) }, OrderByExpr { expr: Identifier("b"), asc: None }], limit: None, offset: None, fetch: None })]
```

## SQL compliance

SQL was first standardized in 1987, and revisions of the standard have been
published regularly since. Most revisions have added significant new features to
the language, and as a result no database claims to support the full breadth of
features. This parser currently supports most of the SQL-92 syntax, plus some
syntax from newer versions that have been explicitly requested, plus some MSSQL-
and PostgreSQL-specific syntax. Whenever possible, the [online SQL:2011
grammar][sql-2011-grammar] is used to guide what syntax to accept. (We will
happily accept changes that conform to the SQL:2016 syntax as well, but that
edition's grammar is not yet available online.)

Unfortunately, stating anything more specific about compliance is difficult.
There is no publicly available test suite that can assess compliance
automatically, and doing so manually would strain the project's limited
resources. Still, we are interested in eventually supporting the full SQL
dialect, and we are slowly building out our own test suite.

If you are assessing whether this project will be suitable for your needs,
you'll likely need to experimentally verify whether it supports the subset of
SQL that you need. Please file issues about any unsupported queries that you
discover. Doing so helps us prioritize support for the portions of the standard
that are actually used. Note that if you urgently need support for a feature,
you will likely need to write the implementation yourself. See the
[Contributing](#Contributing) section for details.

### Supporting custom SQL dialects

This is a work in progress, but we have some notes on [writing a custom SQL
parser](docs/custom_sql_parser.md).

## Design

The core expression parser uses the [Pratt Parser] design, which is a top-down
operator-precedence (TDOP) parser, while the surrounding SQL statement parser is
a traditional, hand-written recursive descent parser. Eli Bendersky has a good
[tutorial on TDOP parsers][tdop-tutorial], if you are interested in learning
more about the technique.

We are a fan of this design pattern over parser generators for the following
reasons:

- Code is simple to write and can be concise and elegant
- Performance is generally better than code generated by parser generators
- Debugging is much easier with hand-written code
- It is far easier to extend and make dialect-specific extensions
  compared to using a parser generator

## Contributing

Contributions are highly encouraged!

Pull requests that add support for or fix a bug in a feature in the SQL
standard, or a feature in a popular RDBMS, like Microsoft SQL Server or
PostgreSQL, will almost certainly be accepted after a brief review. For
particularly large or invasive changes, consider opening an issue first,
especially if you are a first time contributor, so that you can coordinate with
the maintainers. CI will ensure that your code passes `cargo test`,
`cargo fmt`, and `cargo clippy`, so you will likely want to run all three
commands locally before submitting your PR.

If you are unable to submit a patch, feel free to file an issue instead. Please
try to include:

  * some representative examples of the syntax you wish to support or fix;
  * the relevant bits of the [SQL grammar][sql-2011-grammar], if the syntax is
    part of SQL:2011; and
  * links to documentation for the feature for a few of the most popular
    databases that support it.

Please be aware that, while we strive to address bugs and review PRs quickly, we
make no such guarantees for feature requests. If you need support for a feature,
you will likely need to implement it yourself. Our goal as maintainers is to
facilitate the integration of various features from various contributors, but
not to provide the implementations ourselves, as we simply don't have the
resources.

[tdop-tutorial]: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing
[`cargo fmt`]: https://github.com/rust-lang/rustfmt#on-the-stable-toolchain
[current issues]: https://github.com/andygrove/sqlparser-rs/issues
[DataFusion]: https://github.com/apache/arrow/tree/master/rust/datafusion
[LocustDB]: https://github.com/cswinter/LocustDB
[Pratt Parser]: https://tdop.github.io/
[sql-2011-grammar]: https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html
[sql-standard]: https://en.wikipedia.org/wiki/ISO/IEC_9075
