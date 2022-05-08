use std::time::{Duration, Instant};

use super::ResolvedExpr;

pub trait ExprOptimizer {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr>;
}

pub struct StabilizingExprOptimizer<E>(E);

impl<E: ExprOptimizer> ExprOptimizer for StabilizingExprOptimizer<E> {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr> {
        let mut resolved = self.0.optimize(expr)?;

        while let Some(new) = self.0.optimize(&resolved) {
            resolved = new;
        }
        Some(resolved)
    }
}

pub struct LimitedStabilizingExprOptimizer<E> {
    inner: E,
    max_attempts: u32,
    max_duration: Option<Duration>,
}

impl<E: ExprOptimizer> ExprOptimizer for LimitedStabilizingExprOptimizer<E> {
    fn optimize(&self, expr: &ResolvedExpr) -> Option<ResolvedExpr> {
        let max_attempts = self.max_attempts;
        let mut attempts = 0;

        if let Some(duration) = self.max_duration {
            let start = Instant::now();

            let mut resolved = self.inner.optimize(expr)?;
            attempts += 1;

            while attempts < max_attempts && start.elapsed() < duration {
                if let Some(new) = self.optimize(&resolved) {
                    resolved = new;
                    attempts += 1;
                }
            }
            return Some(resolved);
        } else {
            let mut resolved = self.inner.optimize(expr)?;
            attempts += 1;

            while attempts < max_attempts {
                if let Some(new) = self.optimize(&resolved) {
                    resolved = new;
                    attempts += 1;
                } else {
                    return Some(resolved);
                }
            }

            Some(resolved)
        }
    }
}
