//! The macro the points are written with, kept apart from `test_hooks` so that it exists whether
//! or not the feature does.

/// A point in the code a test can hook, used in an `async` function as
/// `test_hook!(crate::test_hooks::points::NAME, scope)?;`. The macro awaits for itself.
///
/// With the `test-hooks` feature off it is an `Ok(())` the optimiser removes. The point's name is
/// never resolved, since the module it names is not compiled, and the scope is only borrowed, so a
/// place expression such as a field or a local costs nothing and warns of nothing. An expression
/// that does work, such as `format!(..)`, is still evaluated, so build the scope beforehand or
/// write a place.
#[cfg(feature = "test-hooks")]
macro_rules! test_hook {
    ($point:expr, $scope:expr) => {
        $crate::test_hooks::hit($point, $scope).await
    };
}

#[cfg(not(feature = "test-hooks"))]
macro_rules! test_hook {
    ($point:expr, $scope:expr) => {{
        let _ = &$scope;
        ::std::result::Result::<(), $crate::sysdb::Error>::Ok(())
    }};
}
