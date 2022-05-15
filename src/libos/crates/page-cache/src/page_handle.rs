use crate::prelude::*;
use crate::Page;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub trait PageKey: Into<usize> + Copy + Send + Sync + Debug + 'static {}

/// Page handle.
#[derive(Clone)]
pub struct PageHandle<K: PageKey, A: PageAlloc>(Arc<Inner<K, A>>);
struct Inner<K: PageKey, A: PageAlloc> {
    key: K,
    pollee: Pollee,
    state_and_page: Mutex<(PageState, Page<A>)>,
}

impl<K: PageKey, A: PageAlloc> PageHandle<K, A> {
    pub(crate) fn new(key: K) -> Self {
        Self(Arc::new(Inner {
            key,
            pollee: Pollee::new(Events::empty()),
            state_and_page: Mutex::new((PageState::Uninit, Page::new().unwrap())),
        }))
    }

    pub fn key(&self) -> K {
        self.0.key
    }

    pub fn pollee(&self) -> &Pollee {
        &self.0.pollee
    }

    pub fn lock(&'a self) -> PageHandleGuard<'a, A> {
        PageHandleGuard(self.0.state_and_page.lock())
    }
}

impl<K: PageKey, A: PageAlloc> Debug for PageHandle<K, A> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let page_guard = self.lock();
        write!(
            f,
            "PageHandle {{ key: {:?}, state: {:?} }}",
            self.key(),
            page_guard.state()
        )
    }
}

pub struct PageHandleGuard<'a, A: PageAlloc>(MutexGuard<'a, (PageState, Page<A>)>);

impl<'a, A: PageAlloc> PageHandleGuard<'a, A> {
    pub fn state(&self) -> PageState {
        self.0 .0
    }

    pub fn set_state(&mut self, new_state: PageState) {
        fn allow_state_transition(curr_state: PageState, new_state: PageState) -> bool {
            match (curr_state, new_state) {
                (_, PageState::Uninit) => false,
                (PageState::Uninit | PageState::Dirty, PageState::UpToDate) => false,
                (PageState::Fetching | PageState::Flushing, PageState::Dirty) => false,
                (state, PageState::Fetching) if state != PageState::Uninit => false,
                (state, PageState::Flushing) if state != PageState::Dirty => false,
                _ => true,
            }
        }
        debug_assert!(allow_state_transition(self.state(), new_state));

        self.0 .0 = new_state;
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0 .1.as_slice()
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self.0 .1.as_slice_mut()
    }

    pub fn page(&mut self) -> &mut Page<A> {
        &mut self.0 .1
    }
}
