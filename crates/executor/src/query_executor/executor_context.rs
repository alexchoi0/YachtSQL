use std::cell::RefCell;
use std::rc::Rc;

use crate::CorrelationContext;
use crate::query_executor::evaluator::physical_plan::SubqueryExecutor;

#[derive(Default)]
pub struct ExecutorContext {
    pub subquery_executor: RefCell<Option<Rc<dyn SubqueryExecutor>>>,
    pub correlation: RefCell<Option<CorrelationContext>>,
}

impl ExecutorContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_subquery_executor(&self, executor: Rc<dyn SubqueryExecutor>) {
        *self.subquery_executor.borrow_mut() = Some(executor);
    }

    pub fn clear_subquery_executor(&self) {
        *self.subquery_executor.borrow_mut() = None;
    }

    pub fn subquery_executor(&self) -> Option<Rc<dyn SubqueryExecutor>> {
        self.subquery_executor.borrow().clone()
    }

    pub fn set_correlation(&self, ctx: CorrelationContext) {
        *self.correlation.borrow_mut() = Some(ctx);
    }

    pub fn clear_correlation(&self) {
        *self.correlation.borrow_mut() = None;
    }

    pub fn correlation(&self) -> Option<CorrelationContext> {
        self.correlation.borrow().clone()
    }
}

pub struct SubqueryExecutorGuard<'a> {
    ctx: &'a ExecutorContext,
    previous: Option<Rc<dyn SubqueryExecutor>>,
}

impl<'a> SubqueryExecutorGuard<'a> {
    pub fn set(ctx: &'a ExecutorContext, executor: Rc<dyn SubqueryExecutor>) -> Self {
        let previous = ctx.subquery_executor.borrow().clone();
        *ctx.subquery_executor.borrow_mut() = Some(executor);
        Self { ctx, previous }
    }
}

impl Drop for SubqueryExecutorGuard<'_> {
    fn drop(&mut self) {
        *self.ctx.subquery_executor.borrow_mut() = self.previous.take();
    }
}

pub struct CorrelationGuard<'a> {
    ctx: &'a ExecutorContext,
    previous: Option<CorrelationContext>,
}

impl<'a> CorrelationGuard<'a> {
    pub fn set(ctx: &'a ExecutorContext, correlation: CorrelationContext) -> Self {
        let previous = ctx.correlation.borrow().clone();
        *ctx.correlation.borrow_mut() = Some(correlation);
        Self { ctx, previous }
    }
}

impl Drop for CorrelationGuard<'_> {
    fn drop(&mut self) {
        *self.ctx.correlation.borrow_mut() = self.previous.take();
    }
}
