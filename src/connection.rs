use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use bytes::Buf;
use mini_redis::{Frame, Result};
use mini_redis::frame::Error::Incomplete;
use std::io::Cursor;

struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    // other fields
}

impl Connection {

    pub fn new (stream: TcpStream) -> Connection {
        Conection {
            stream: BufWriter<TcpStream>,
            buffer: BytesMut,
        }
    }

    /// Read a frame from the connection
    ///
    /// Returns `None` if EOF is reached
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from buffered data
            // If enough data has been buffered, the frame is returned
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Not enough buffered data to read a frame
            // Attempt to read more data from the socket
            //
            // On success, the number of bytes is returned. `0`
            // indicates "end of stream"
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        // Check whether a full frame is available
        match Frame::check(&mut buf) {
            Ok(_) => {
                // Get byte length of the frame
                let len = buf.position() as usize;

                // Reset internal cursor for the call to `parse`
                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;
                
                // Discard frame from the buffer
                self.buffer.advance(len);

                // Return frame to the caller
                Ok(Some(frame))
            }
            // Not enough data has been buffered
            Err(Incomplete) => Ok(None),
            // An error was encountered
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!(),
        }
        self.stream.flush().await;

        Ok(())
    }
}
