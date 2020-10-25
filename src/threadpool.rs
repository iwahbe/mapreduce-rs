use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

/// A que of jobs and workers to execute them.
///
/// A [`ThreadPool`] maintains list of workers, and keeps track of the number of
/// queued jobs.
///
/// ```
/// let pool = ThreadPool::new(3).unwrap() // thread pool has 3 workers
///                                        // unwrap to prevent passing 0 threads
/// pool.execute(|| println!("Task 1"));
/// pool.execute(|| println!("Task 2"));
/// pool.execute(|| println!("Task 3"));
///
/// pool.wait(1);
/// println!("There can only be 1 thread outstanding now.");
/// ```
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
    count: Arc<AtomicUsize>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A message for a worker.
///
/// The worker either has a new job or, the worker shouold terminate.
enum Message {
    NewJob(Job),
    Terminate,
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// Adapted from when I read the rust book.
    pub fn new(size: usize) -> Result<ThreadPool, ()> {
        if size == 0 {
            return Err(());
        }
        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        let count = Arc::new(AtomicUsize::new(0));

        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver), count.clone()));
        }

        Ok(ThreadPool {
            workers,
            sender,
            count,
        })
    }

    /// Runs f on the first available worker.
    ///
    /// Sends the closure `f` to be executed on some thread in the thread pool.
    /// Unless `wait` is called, there is no garentee that `f` will be called
    /// until the pool drops, after which all tasks will be finished.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.count.fetch_add(1, Ordering::SeqCst);

        self.sender.send(Message::NewJob(job)).unwrap();
    }

    /// Block until there are tasks queued less then or equal to `tasks` in
    /// number.
    ///
    /// Simply exectues a while loop conditional on the count of tasks
    /// remaining.
    ///
    /// ```
    /// # let pool = ThreadPool::new(5).unwrap();
    /// add_five_tasks(&pool); // there are between 0 and 5 tasks left
    /// pool.wait(3)           // there are between 0 and 3 tasks left
    /// pool.wait(0)           // there are tasks remaining
    /// ```
    #[allow(dead_code)]
    pub fn wait(&self, tasks: usize) {
        while self.count.load(Ordering::SeqCst) > tasks {}
    }

    pub fn join(self) {
        drop(self)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<Mutex<mpsc::Receiver<Message>>>, count: Arc<AtomicUsize>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    job();
                    count.fetch_sub(1, Ordering::SeqCst);
                }
                Message::Terminate => {
                    break;
                }
            }
        });

        Worker {
            thread: Some(thread),
        }
    }
}

pub struct ReducePool {
    // Consider what part of your interface this is
    // supporting.
    count: Arc<AtomicUsize>,

    // Your worker threads will need to persist past calls to execute. Because
    // you need to store threads, you need to implement a drop function to
    // facilitate cleanup.
    _workers: (),
}

impl ReducePool {
    // All rust code should be run through a tool called rustfmt. This is the
    // community recognized code formater. Any rust IDE (including vscode,
    // emacs, atom...) can be set up to run rustfmt when you save. I would encourage this.
    /// Executes each closure in the `funcs` iterator in a single non-blocking thread.
    ///
    /// This means that each call to execute will generate exactly 1 thread
    /// (plus whatever the closures do).
    pub fn execute<I, F>(&self, funcs: I)
    // This was marked as mut, which broke the build. Make sure that your
    // code compiles before you submit it to github. This is especially
    // important for rust, where getting things to compile is harder then
    // other languages (because of the static analysis pass).
    where
        I: Iterator<Item = F>,
        F: FnOnce() + Send + 'static,
    {
        let mut count = 0; // Because count is not used in the threads
                           // themselves, it can just be a usize.

        // Call each func in a thread.
        let mut children = Vec::new(); // This vs vec![] is just style, but I
                                       // like it better because the interface
                                       // is consistant with all containers.
        for f in funcs {
            // We want each thread to handle it's own iterator. That would
            // result in 1 thread for each call to execute, which is what we
            // want. This results in one thread for each key in each iterable.
            children.push(thread::spawn(|| f()));
            self.count.fetch_add(1, Ordering::SeqCst);
            count += 1;
        }

        for t in children {
            // We don't want to join our threads, as that prevents us from
            // spawning the next thread until this thread is done.
            let _ = t.join();
            self.count.fetch_sub(1, Ordering::SeqCst);
            count -= 1;
        }
        assert_eq!(count, 0); // On the other hand, you don't do anything with
                              // count, because the entire process happens synchronously.
    }
    /// Wait until there are `tasks` tasks outstanding.
    pub fn wait(&self, tasks: usize) {
        while self.count.load(Ordering::SeqCst) > tasks {}
    }

    /// Creates a new instance of `ReducePool`.
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            _workers: (),
        }
    }
}
