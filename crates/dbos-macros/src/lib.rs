//! Procedural macros for [DBOS Transact](https://docs.dbos.dev/). Not used directly: everything
//! here is re-exported from the `dbos` crate, which is where it is documented.
//!
//! # Why a procedural macro
//!
//! One macro lives here, and it is here because of a single limitation: a `macro_rules!` can
//! neither invent an identifier nor count. A durable race has to name a slot per branch and match
//! on a branch's index, and a declarative macro can do neither — so it would have to hand the work
//! to a function written per arity, with a sum type per arity to carry the winner's value back
//! out. That caps the fan-out at however many arities somebody wrote, and puts the shape of the
//! expansion in two places.
//!
//! A procedural macro is an ordinary Rust program over tokens, so it writes `__dbos_branch0`,
//! `__dbos_branch1`, … for as many branches as it was given, and there is no arity to run out of.
//! Two smaller wins come with it: `match`'s comma rule, which an `expr` fragment cannot express
//! because it does not backtrack, and refusals spanned at the offending tokens rather than one
//! blanket message about the whole invocation.
//!
//! What it costs is `syn` and `quote` in the build graph, which is why the `dbos` crate puts this
//! behind a feature.

use proc_macro::TokenStream;
use proc_macro2::{Literal, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Error, Expr, Pat, Token, parse_macro_input};

/// A durable race over steps. Documented on `dbos::select_step`, which re-exports it.
#[proc_macro]
pub fn select_step(input: TokenStream) -> TokenStream {
    let race = parse_macro_input!(input as Race);
    expand(&race).into()
}

/// The arms of one `select_step!`, in source order — which is polling order, and the order the
/// recorded index counts in.
struct Race {
    arms: Vec<Arm>,
}

/// `binding = step => expression`.
struct Arm {
    /// The winner's outcome, bound for the body. A `Result`, so a body decides for itself whether
    /// to `?` it, match it, or report it.
    binding: Pat,
    /// The durable call. One expression, evaluated once, and evaluated before any branch is
    /// polled.
    step: Expr,
    /// What to run if this branch wins.
    body: Expr,
}

impl Parse for Race {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut arms = Vec::<Arm>::new();
        while !input.is_empty() {
            let arm = input.parse::<Arm>()?;
            // `match`'s rule exactly: a block-shaped body ends itself, so the comma after it is
            // optional. Worth the dozen lines, because the alternative is a macro that reads like
            // `match` until somebody writes a block and then does not.
            let terminated = !ends_itself(&arm.body);
            arms.push(arm);
            if input.is_empty() {
                break;
            }
            if terminated {
                input.parse::<Token![,]>()?;
            } else {
                input.parse::<Option<Token![,]>>()?;
            }
        }

        if arms.len() < 2 {
            // Spanned on the lone branch where there is one, so the error points at the code
            // rather than at the macro's name.
            let at = arms
                .first()
                .map_or_else(Span::call_site, |arm| arm.step.span());
            return Err(Error::new(
                at,
                "select_step! races two or more durable calls. One branch is not a race — await \
                 the call directly, which is both shorter and durable on its own.",
            ));
        }

        Ok(Race { arms })
    }
}

impl Parse for Arm {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        refuse_at_arm_start(input)?;

        let binding = Pat::parse_multi_with_leading_vert(input)?;
        if !always_matches(&binding) {
            return Err(Error::new(binding.span(), REFUTABLE));
        }
        if input.peek(Token![=>]) {
            return Err(Error::new(
                input.span(),
                "each arm of select_step! binds its branch's outcome: `binding = call => \
                 expression`. The binding is a `Result`, because a branch can fail and the arm is \
                 what decides whether that failure ends the workflow.",
            ));
        }
        input.parse::<Token![=]>()?;

        let step = input.parse::<Expr>()?;
        refuse_guard(input)?;
        input.parse::<Token![=>]>()?;

        let body = input.parse::<Expr>()?;
        Ok(Arm {
            binding,
            step,
            body,
        })
    }
}

/// The three things tokio's `select!` has that a durable race must not, caught where they are
/// written rather than as one blanket "this did not parse".
///
/// Each is refused for the same reason: **the checkpoint records a position**, and every one of
/// these lets the set of positions differ between the first run and the replay.
fn refuse_at_arm_start(input: ParseStream) -> syn::Result<()> {
    if input.peek(Token![else]) {
        return Err(Error::new(
            input.span(),
            "select_step! has no `else` arm. `else` runs when every branch is disabled, which \
             only happens with guards — and a guard changes which branches exist between runs, \
             where the recorded winner is a position among them.",
        ));
    }
    if input.peek(Token![if]) {
        return Err(Error::new(input.span(), GUARD));
    }
    if input.peek(syn::Ident) && input.peek2(Token![;]) {
        let word = input.fork().parse::<syn::Ident>()?;
        if word == "biased" {
            return Err(Error::new(
                word.span(),
                "select_step! is always biased: branches are polled in source order, and that is \
                 the contract rather than an option. Tokio randomises for fairness, and fairness \
                 is the one property a replay cannot reproduce.",
            ));
        }
    }
    if input.peek(syn::Ident) && input.peek2(Token![=>]) {
        let word = input.fork().parse::<syn::Ident>()?;
        if word == "complete" {
            return Err(Error::new(
                word.span(),
                "select_step! has no `complete` arm: exactly one branch wins and the losers are \
                 dropped, so there is no state in which all of them have finished.",
            ));
        }
    }
    Ok(())
}

/// Both spellings of a guard: tokio's `= call, if cond =>` and `match`'s `= call if cond =>`.
fn refuse_guard(input: ParseStream) -> syn::Result<()> {
    let guard = input.peek(Token![if]) || (input.peek(Token![,]) && input.peek2(Token![if]));
    if guard {
        return Err(Error::new(input.span(), GUARD));
    }
    Ok(())
}

const GUARD: &str = "select_step! takes no guards. A branch that is present on the first run and \
                     absent on the replay would make the recorded winner name a branch that is no \
                     longer there — so the set of branches has to be a property of the code, not \
                     of a condition. Decide before the race and pick the call, or race one that \
                     returns early.";

/// Whether a pattern always matches, which an arm's binding has to.
///
/// The expansion binds with `let`, so a pattern that can fail has nowhere to fall to — and left to
/// `rustc` it is an `E0005` about a `let` the caller never wrote, with a `let...else` suggestion
/// pointing into the expansion. Refused here instead, at the pattern.
///
/// One shape gets past it: a bare `None`, which is a unit variant to `rustc` and an ordinary
/// binding to a parser that cannot resolve names. That one still lands on `E0005`, and it is not
/// the spelling anybody reaches for over a `Result`.
fn always_matches(pat: &Pat) -> bool {
    match pat {
        Pat::Wild(_) | Pat::Rest(_) => true,
        Pat::Ident(name) => name
            .subpat
            .as_ref()
            .is_none_or(|(_, sub)| always_matches(sub)),
        Pat::Paren(inner) => always_matches(&inner.pat),
        Pat::Reference(inner) => always_matches(&inner.pat),
        Pat::Type(inner) => always_matches(&inner.pat),
        Pat::Tuple(tuple) => tuple.elems.iter().all(always_matches),
        _ => false,
    }
}

const REFUTABLE: &str = "select_step! binds its branch's whole outcome, so the binding is a name \
                         rather than a pattern that can fail to match — there is no other arm for \
                         it to fall to when it does not. `tokio::select!` allows one because a \
                         branch that fails the pattern is simply disabled, and a disabled branch \
                         is a branch the replay would not find. Bind the `Result` and take it \
                         apart in the body: `outcome = call => match outcome { .. }`, or \
                         `outcome?` to let a failure end the workflow.";

/// Whether a body needs no comma after it, which is `match`'s rule and the list `rustc` uses for
/// it: an expression that ends in a block ends the arm too.
fn ends_itself(body: &Expr) -> bool {
    matches!(
        body,
        Expr::Block(_)
            | Expr::Const(_)
            | Expr::ForLoop(_)
            | Expr::If(_)
            | Expr::Loop(_)
            | Expr::Match(_)
            | Expr::TryBlock(_)
            | Expr::Unsafe(_)
            | Expr::While(_)
    )
}

/// Writes the race out: build every branch, ask for a recorded winner, poll or replay, run one arm.
///
/// The generated locals are spanned at [`Span::mixed_site`], so they resolve at the *definition*
/// site and cannot collide with anything the caller named — while the arms' patterns and bodies
/// keep the caller's spans, which is what lets a binding introduced by one be seen by the other.
fn expand(race: &Race) -> TokenStream2 {
    let count = race.arms.len();
    let branch: Vec<_> = (0..count)
        .map(|at| format_ident!("__dbos_branch{at}", span = Span::mixed_site()))
        .collect();
    let slot: Vec<_> = (0..count)
        .map(|at| format_ident!("__dbos_slot{at}", span = Span::mixed_site()))
        .collect();
    let index: Vec<_> = (0..count).map(Literal::usize_suffixed).collect();

    let step: Vec<&Expr> = race.arms.iter().map(|arm| &arm.step).collect();
    let binding: Vec<&Pat> = race.arms.iter().map(|arm| &arm.binding).collect();
    let body: Vec<&Expr> = race.arms.iter().map(|arm| &arm.body).collect();

    let branches = format_ident!("__dbos_branches", span = Span::mixed_site());
    let winner = format_ident!("__dbos_winner", span = Span::mixed_site());
    let at = format_ident!("__dbos_at", span = Span::mixed_site());
    let cx = format_ident!("__dbos_cx", span = Span::mixed_site());
    let value = format_ident!("__dbos_value", span = Span::mixed_site());
    let failed = format_ident!("__dbos_failed", span = Span::mixed_site());
    let recording = format_ident!("__dbos_recording", span = Span::mixed_site());

    quote! {{
        // **Every branch is built before any is polled.** A durable call takes its id when it is
        // built, so this is what fixes the ids: they follow source order, which is the order a
        // replay builds them in again.
        #( let mut #branch = #step; )*

        // Read while the branches are alive, because the losers are dropped as soon as the race is
        // decided and a stale-winner report has to be able to name a branch that is gone. `push`
        // is also what ties the branches to one error type: they differ in what they return and
        // they agree on how they fail.
        let mut #branches = ::dbos::__private::Branches::new();
        #( #branches.push(&#branch); )*

        // One slot per branch, so the winner's value comes out of the poll loop with its own type
        // rather than through a sum type invented per arity. This is the whole of what the
        // procedural macro buys: it can write as many distinct names as it was given branches.
        #( let mut #slot = ::core::option::Option::None; )*

        let #winner = match ::dbos::__private::check_select(&#branches).await {
            ::core::result::Result::Err(#failed) => ::core::result::Result::Err(#failed),
            // **Only the winner is polled.** The race is already decided, so nothing a loser
            // would do now is part of it: one that recorded nothing would be running for the
            // first time, and a `sleep` — the one branch that leaves a row without having
            // finished — would serve out a wait nobody is waiting on. They were still *built*,
            // which is what keeps their ids spent and the numbering stable.
            ::core::result::Result::Ok(::dbos::__private::Racing::Replay(#at)) => {
                match #at {
                    #(
                        #index => #slot = ::core::option::Option::Some(#branch.await),
                    )*
                    // `check_select` holds a recorded winner to the branch count before it
                    // returns, so an index outside the set has already been reported.
                    _ => ::core::unreachable!(
                        "check_select returned a branch outside the {} it was given",
                        #count
                    ),
                }

                // Checked on the replay exactly as on a fresh race. A replayed branch still reads
                // its own row, so a cancellation, an interruption or a database failure arrives
                // here too — and handing one to the arm would let a transient failure decide what
                // the arm did, which is the divergence the recorded winner exists to prevent.
                // Nothing is recorded either way: this select already has its row.
                let #failed = match #at {
                    #( #index => ::dbos::__private::control_error(&mut #slot), )*
                    _ => ::core::option::Option::None,
                };
                match #failed {
                    ::core::option::Option::Some(#failed) => ::core::result::Result::Err(#failed),
                    ::core::option::Option::None => ::core::result::Result::Ok(#at),
                }
            }
            ::core::result::Result::Ok(::dbos::__private::Racing::Fresh(#recording)) => {
                // `Pin::new` rather than `Box::pin`: a `PendingStep` is `Unpin`, which it
                // documents as contract precisely so a combinator need not pin each branch.
                let #at = ::core::future::poll_fn(|#cx| {
                    #(
                        // Returned from inside the loop, so nothing is polled after one went
                        // `Ready`. Fixed source order, never tokio's randomised one: fairness is
                        // exactly the property a replay cannot reproduce, so two branches ready in
                        // the same instant resolve to the earlier — and a replay reads the winner
                        // rather than racing, which makes that a tie-break rather than something
                        // to depend on.
                        if let ::core::task::Poll::Ready(#value) = ::core::future::Future::poll(
                            ::core::pin::Pin::new(&mut #branch), #cx
                        ) {
                            #slot = ::core::option::Option::Some(#value);
                            return ::core::task::Poll::Ready(#index);
                        }
                    )*
                    ::core::task::Poll::Pending
                }).await;

                // Dropped before the checkpoint is written, so every loser is stopped at its next
                // suspension point, its destructors have run, and a losing step's cancellation
                // token has fired for any work it handed off, before anything records that the
                // race is over. Whatever a loser did before that, it did once — and did it
                // invisibly wherever the row is written only at the end, which is everything but a
                // `sleep`: that one records the instant it will wake at before waiting on it, so a
                // losing sleep leaves a row for a wait that was abandoned. The same trade a step
                // timeout makes.
                ::core::mem::drop(( #( #branch, )* ));

                // A control signal — a cancellation, an interruption, a database failure — is not
                // the race's decision, and is returned with nothing recorded, exactly as the call
                // that produced it returned it. See `control_error`.
                let #failed = match #at {
                    #( #index => ::dbos::__private::control_error(&mut #slot), )*
                    _ => ::core::option::Option::None,
                };
                match #failed {
                    ::core::option::Option::Some(#failed) => ::core::result::Result::Err(#failed),
                    ::core::option::Option::None => {
                        match ::dbos::__private::record_select(#recording, #at).await {
                            ::core::result::Result::Ok(()) => ::core::result::Result::Ok(#at),
                            ::core::result::Result::Err(#failed) => {
                                ::core::result::Result::Err(#failed)
                            }
                        }
                    }
                }
            }
        };

        // One `match` over the index, rather than the arm bodies appearing once per path: a replay
        // and a fresh race reach the same place with the same slot filled.
        match #winner {
            ::core::result::Result::Err(#failed) => ::core::result::Result::Err(#failed),
            #(
                ::core::result::Result::Ok(#index) => {
                    let #binding = match #slot {
                        ::core::option::Option::Some(#value) => #value,
                        ::core::option::Option::None => ::core::unreachable!(
                            "branch {} won without leaving its result", #index
                        ),
                    };
                    ::core::result::Result::Ok(#body)
                }
            )*
            ::core::result::Result::Ok(_) => ::core::unreachable!(
                "the race resolved to a branch outside the {} it was given", #count
            ),
        }
    }}
}

/// The grammar and the shape of the expansion, tested without a compiler around them.
///
/// A `#[proc_macro]` cannot be called from a test — `proc_macro::TokenStream` only exists while
/// the compiler is expanding something — but everything below that entry point deals in
/// `proc_macro2::TokenStream`, which is an ordinary value. So the two halves that hold the design
/// are reachable: what the macro refuses, and what it writes.
///
/// What this cannot see is whether the expansion *compiles*, which is what the tests over in
/// `dbos` are for.
#[cfg(test)]
mod tests {
    use super::*;

    /// The message a refusal reports, with the invocation's braces left off — `Race` parses the
    /// arms, not the delimiters the compiler has already stripped.
    fn refuse(arms: &str) -> String {
        let tokens: TokenStream2 = arms.parse().expect("the source must tokenize");
        match syn::parse2::<Race>(tokens) {
            Ok(race) => panic!("expected a refusal, got {} arms", race.arms.len()),
            Err(failed) => failed.to_string(),
        }
    }

    fn accept(arms: &str) -> Race {
        let tokens: TokenStream2 = arms.parse().expect("the source must tokenize");
        syn::parse2::<Race>(tokens).expect("this should have parsed")
    }

    /// **One branch is not a race**, and the refusal is at compile time rather than at run time,
    /// which is the one thing a macro can do that a `Vec`-taking function could not.
    #[test]
    fn fewer_than_two_branches_is_refused() {
        assert!(refuse("x = one() => x?").contains("races two or more durable calls"));
        assert!(refuse("").contains("races two or more durable calls"));
    }

    /// Both spellings, because someone arriving from `tokio::select!` writes one and someone
    /// arriving from `match` writes the other, and a guard is refused for the same reason either
    /// way: the checkpoint records a *position* among the branches that exist.
    #[test]
    fn a_guard_is_refused_in_either_spelling() {
        assert!(refuse("x = one(), if flag => x?, y = two() => y?").contains("takes no guards"));
        assert!(refuse("x = one() if flag => x?, y = two() => y?").contains("takes no guards"));
    }

    /// The three tokio spellings that have no meaning in a durable race, each named.
    #[test]
    fn the_tokio_only_arms_are_refused_by_name() {
        assert!(refuse("biased; x = one() => x?, y = two() => y?").contains("always biased"));
        assert!(refuse("x = one() => x?, y = two() => y?, else => 0").contains("no `else` arm"));
        assert!(
            refuse("x = one() => x?, y = two() => y?, complete => 0").contains("no `complete` arm")
        );
    }

    /// `=>` where `=` belongs — the arm reads as a `match` and is not one, so it says which half
    /// is missing rather than reporting a failed parse.
    #[test]
    fn an_arm_without_its_binding_is_refused() {
        assert!(refuse("x => one(), y = two() => y?").contains("binds its branch's outcome"));
    }

    /// The comma rule is `match`'s, which means it is only *sometimes* optional — and the message
    /// when it is not is `match`'s too.
    #[test]
    fn a_missing_comma_after_a_plain_body_is_refused() {
        assert!(refuse("x = one() => x? y = two() => y?").contains("expected `,`"));
    }

    /// A body that ends in a block ends its arm, including mid-list — the thing a declarative form
    /// would have had to demand a comma for.
    #[test]
    fn a_block_body_needs_no_comma() {
        assert_eq!(accept("x = one() => { x? } y = two() => y?").arms.len(), 2);
        assert_eq!(
            accept("x = one() => match x { _ => 0 } y = two() => y?")
                .arms
                .len(),
            2
        );
        assert_eq!(accept("x = one() => x?, y = two() => y?,").arms.len(), 2);
    }

    /// **No arity to run out of**, which is the whole reason this is a procedural macro.
    #[test]
    fn there_is_no_upper_bound_on_branches() {
        let arms = (0..40)
            .map(|at| format!("b{at} = step{at}() => b{at}?"))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(accept(&arms).arms.len(), 40);
        // The last slot exists and the one past it does not, rather than a count of appearances:
        // what matters is that the names go as wide as the arms, not how often each is written.
        let expanded = expand(&accept(&arms)).to_string();
        assert!(expanded.contains("__dbos_slot39"));
        assert!(!expanded.contains("__dbos_slot40"));
    }

    /// The expansion reaches the checkpoint rather than reimplementing it, and it names a slot per
    /// branch — the two claims the module doc makes about what is here and what is in `dbos`.
    #[test]
    fn the_expansion_names_a_slot_per_branch_and_calls_the_core() {
        let expanded = expand(&accept(
            "x = one() => x?, y = two() => y?, z = three() => z?",
        ))
        .to_string();
        for called in [
            "Branches",
            "check_select",
            "record_select",
            "Racing",
            "poll_fn",
        ] {
            assert!(
                expanded.contains(called),
                "the expansion never reaches {called}"
            );
        }
        for at in 0..3 {
            assert!(expanded.contains(&format!("__dbos_slot{at}")));
            assert!(expanded.contains(&format!("__dbos_branch{at}")));
        }
        assert!(
            !expanded.contains("__dbos_slot3"),
            "a fourth branch was invented"
        );
    }

    /// **Each arm's body is written once**, which is why the replay path and the fresh race meet
    /// at one `match` on the index instead of each carrying a copy of every arm.
    #[test]
    fn an_arm_body_appears_once_in_the_expansion() {
        let expanded = expand(&accept(
            "x = one() => marker_a(x), y = two() => marker_b(y)",
        ))
        .to_string();
        assert_eq!(expanded.matches("marker_a").count(), 1);
        assert_eq!(expanded.matches("marker_b").count(), 1);
    }

    /// A refutable binding is the `tokio::select!` reflex, and it is refused at the pattern —
    /// where `rustc` would have reported `E0005` against a `let` in code the caller never wrote.
    #[test]
    fn a_refutable_binding_is_refused() {
        for arms in [
            "Ok(a) = one() => a, y = two() => y?",
            "Err(e) = one() => 0, y = two() => y?",
            "1 = one() => 0, y = two() => y?",
            "a | b = one() => 0, y = two() => y?",
            "got @ Ok(_) = one() => got, y = two() => y?",
        ] {
            assert!(refuse(arms).contains("whole outcome"), "{arms}");
        }
    }

    /// The bindings that are not a bare name but still always match are left alone — the refusal
    /// is about matching, not about spelling.
    #[test]
    fn an_irrefutable_binding_is_accepted() {
        assert_eq!(accept("_ = one() => 0, y = two() => y?").arms.len(), 2);
        assert_eq!(accept("mut x = one() => x?, y = two() => y?").arms.len(), 2);
        assert_eq!(accept("(a, b) = one() => 0, y = two() => y?").arms.len(), 2);
    }
}
