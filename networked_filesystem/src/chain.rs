use std::{marker::PhantomData, pin::Pin};

use crate::{FileFrameStatus, FileHandleStatus, FrameCommons, FrameHandler, RemoteFileSystem, StreamReceiver, StreamableFileSystemErrors};

pub struct HNil<S, F>{
    _marker2: PhantomData<fn(S, F)>,
}

pub struct HCons<T, F, Tail, S, FT> {
    _marker2: PhantomData<fn(S, FT)>,
    _marker: PhantomData<T>,
    f: F,
    tail: Tail,
}

pub struct Here;
pub struct There<Idx>(PhantomData<Idx>);

pub trait InsertOrReplace<T, F, Idx, S, FT> {
    type Output;
    fn insert_or_replace(&self, f: F) -> Self::Output;
}

impl<T, F, S, FT> InsertOrReplace<T, F, Here, S, FT> for HNil<S, FT> {
    type Output = HCons<T, F, HNil<S, FT>, S, FT>;
    fn insert_or_replace(&self, f: F) -> Self::Output {
        HCons { _marker: PhantomData, f, tail: HNil { _marker2: PhantomData }, _marker2: PhantomData }
    }
}

impl<T, F, OldF, Tail: Clone, S, FT> InsertOrReplace<T, F, Here, S, FT> for HCons<T, OldF, Tail, S, FT> {
    type Output = HCons<T, F, Tail, S, FT>;
    fn insert_or_replace(&self, f: F) -> Self::Output {
        HCons { _marker: PhantomData, f, tail: self.tail.clone(), _marker2: PhantomData }
    }
}

impl<T, F, Head, HeadF: Clone, Tail, Idx, S, FT> InsertOrReplace<T, F, There<Idx>, S, FT> for HCons<Head, HeadF, Tail, S, FT>
where
    Tail: InsertOrReplace<T, F, Idx, S, FT>,
{
    type Output = HCons<Head, HeadF, Tail::Output, S, FT>;
    fn insert_or_replace(&self, f: F) -> Self::Output {
        HCons { _marker: self._marker, f: self.f.clone(), tail: self.tail.insert_or_replace(f), _marker2: PhantomData }
    }
}

pub struct ChainBuilder<'a, H, S, FT> {
    pub fs: &'a mut RemoteFileSystem<S, FT>,
    list: H,
}

impl <'a, S, FT>ChainBuilder<'a, HNil<S, FT>, S, FT> {
    pub fn new(fs: &'a mut RemoteFileSystem<S, FT> ) -> Self {
        ChainBuilder { list: HNil { _marker2: PhantomData }, fs }
    }
}
pub trait Execute {
    type State;
    type FileType;
    async fn execute(&self, state_id: u8, bytes: Vec<u8>, fs: &mut RemoteFileSystem<Self::State, Self::FileType>) -> Result<(), StreamableFileSystemErrors>;
}

impl <S, F>Execute for HNil<S, F> {
    type State = S;
    type FileType = F;
    async fn execute(&self, _state_id: u8, bytes: Vec<u8>, fs: &mut RemoteFileSystem<S, F>) -> Result<(), StreamableFileSystemErrors> {
        Ok(())
    }
}


impl<T: FrameCommons, F, Tail, S: StreamReceiver, FT> Execute for HCons<T, F, Tail, S, FT>
where
    F: for<'a> Fn(u8, T::Output, &'a mut RemoteFileSystem<S, FT>) -> Pin<Box<dyn Future<Output = Result<(), FileHandleStatus>> + Send + 'a>>,
    Tail: Execute<State = S, FileType = FT>,
{
    type State = S;
    type FileType = FT;
    async fn execute(&self, state_id: u8, bytes: Vec<u8>, fs: &mut RemoteFileSystem<S, FT>) -> Result<(), StreamableFileSystemErrors> {
        match T::decode(bytes.clone()) {
            Ok(output) => {
                (self.f)(state_id, output, fs).await.map_err(|e| {
                    match e {
                        FileHandleStatus::NoStateGiven => StreamableFileSystemErrors::NoStateGiven,
                        FileHandleStatus::IncorrectData => StreamableFileSystemErrors::IncorrectData,
                        FileHandleStatus::IncorrectStateAsked => StreamableFileSystemErrors::IncorrectStateAsked,
                    }
                })?;
            },
            Err(e) => {
                return Err(StreamableFileSystemErrors::Unknown);
            },
        }
        self.tail.execute(state_id, bytes, fs).await
    }
}

impl<H: Execute<State = S, FileType = FT>, S: StreamReceiver, FT> ChainBuilder<'_, H, S, FT> {
    pub fn chain<T: FrameCommons, F, Idx>(&mut self, f: F) -> ChainBuilder<H::Output, S, FT>
    where
        H: InsertOrReplace<T, F, Idx, S, FT>,
        F: for<'a> Fn(u8, T::Output, &'a mut RemoteFileSystem<S, FT>) -> Pin<Box<dyn Future<Output = Result<(), FileHandleStatus>> + Send + 'a>>,
    {
        ChainBuilder { list: self.list.insert_or_replace(f), fs: self.fs }
    }
    pub async fn run(&mut self, state_id: u8) -> Result<(), StreamableFileSystemErrors> {
        let remainder = &mut 0;

        loop {
            match self.fs.state.get_chunk().await {
                Ok(bytes) => {
                    let mut total_bytes = Vec::new();
                    total_bytes.extend(self.fs.remainder.clone());        
                    self.fs.remainder = Vec::new();
                    total_bytes.extend(bytes);
                    let mut frame = self.fs.state.create_frame_handler();
                    frame.set_chunks(self.fs.remainder.clone());

                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                let chunks = frame.get_chunks();
                                let _ = self.list.execute(state_id, chunks.clone(), &mut self.fs).await;
                            }
                            if let Some(last_frame) = frames.iter().last() {
                                self.fs.remainder.extend(last_frame.get_remainder().clone());
                            }
                                                        }
                        Err(e) => {
                            match e {
                                FileFrameStatus::FrameNoBegins => {
                                }
                                FileFrameStatus::FrameNoEnds => {
                                    let remainder = total_bytes;
                                    self.fs.remainder = remainder.to_vec();
                                }
                                FileFrameStatus::NotValidFrame
                                | FileFrameStatus::NoFrameDecoding
                                | FileFrameStatus::NotCorrectFrame => {
                                    //return Err(StreamableFileSystemErrors::Unknown);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(StreamableFileSystemErrors::Unknown);
                },
            }
        }
    }
    // pub async fn forward(self, bytes: Vec<u8>){
        
    // }

}

// pub fn foo() -> ChainBuilder<HNil> {
//     ChainBuilder::new()
// }