use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TempTree {
    root: PathBuf,
}

impl TempTree {
    pub(crate) fn new(label: &str) -> Self {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "clinker-dependency-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create isolated dependency policy test tree");
        Self { root }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn path(&self, relative: impl AsRef<Path>) -> PathBuf {
        self.root.join(relative)
    }

    pub(crate) fn write(&self, relative: impl AsRef<Path>, contents: &str) {
        let path = self.path(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create dependency policy fixture parent");
        }
        fs::write(path, contents).expect("write dependency policy fixture");
    }

    pub(crate) fn copy_from_repository(&self, relative: impl AsRef<Path>) {
        let relative = relative.as_ref();
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let source = repository.join(relative);
        let destination = self.path(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).expect("create copied fixture parent");
        }
        fs::copy(source, destination).expect("copy repository contract fixture");
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
