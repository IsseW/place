use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::PathBuf,
};

use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::read::GzDecoder;

pub const NUM_FILES: usize = 78;

#[derive(Debug)]
pub enum Error {
    NonexistentFile,
    Io(io::Error),
    Web(reqwest::Error),
    Deserialize,
}

pub type Result<T> = std::result::Result<T, Error>;

pub enum Fill {
    One { x: u32, y: u32 },
    Rect { x1: u32, y1: u32, x2: u32, y2: u32 },
}

pub struct Pixel {
    pub timestamp: DateTime<Utc>,
    pub user_id: u64,
    pub color: [u8; 3],
    pub fill: Fill,
}

async fn download_file(num: usize) -> Result<Vec<u8>> {
    let target = format!("https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-{:#012}.csv.gzip", num);
    let response = reqwest::get(target).await.map_err(Error::Web)?;
    let content = response.bytes().await.map_err(Error::Web)?;
    Ok(content.to_vec())
}

pub async fn load_file<P>(num: usize, dir: Option<P>) -> Result<Vec<Pixel>>
where
    PathBuf: From<P>,
{
    if num >= NUM_FILES {
        return Err(Error::NonexistentFile);
    }

    let data = if let Some(dir) = dir {
        // If file is in the specified directory, load it.
        let dir = PathBuf::from(dir);

        fs::create_dir_all(&dir).map_err(Error::Io)?;

        let file_path = dir.join(format!("canvas_{:#02}.csv.gzip", num));
        println!("Trying to load file {}.", num);
        match fs::read(&file_path) {
            Ok(data) => {
                println!("Loaded file {}.", num);
                data
            }
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => {
                    println!("File {} not found, downloading.", num);
                    // If file doesn't exist download it and save it.
                    let content = download_file(num).await?;
                    println!("Content downloaded, saving into file {}.", num);
                    let mut dest = fs::File::create(&file_path).map_err(Error::Io)?;
                    dest.write_all(&content).map_err(Error::Io)?;
                    println!("Cache saved for {}.", num);

                    content
                }
                _ => return Err(Error::Io(error)),
            },
        }
    } else {
        println!("Downloading {}", num);
        download_file(num).await?
    };

    let mut decoder = GzDecoder::new(&*data);

    let mut csv_data = String::new();
    println!("Unzipping {}.", num);
    decoder.read_to_string(&mut csv_data).map_err(Error::Io)?;
    let mut res = Vec::new();
    for result in csv_data.lines().skip(1).map(|line| {
        let (timestamp, rest) = line.split_once(',')?;
        let timestamp = &timestamp[..timestamp.len() - 4];

        let timestamp = DateTime::<Utc>::from_utc(
            NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f").ok()?,
            Utc,
        );

        let (user_hash, rest) = rest.split_once(',')?;

        let mut hasher = &mut DefaultHasher::new();
        user_hash.hash(&mut hasher);
        let user_id = hasher.finish();

        let (color, fill) = rest.split_once(',')?;
        let color = [
            u8::from_str_radix(&color[1..3], 16).ok()?,
            u8::from_str_radix(&color[3..5], 16).ok()?,
            u8::from_str_radix(&color[5..7], 16).ok()?,
        ];

        let parts = fill[1..fill.len() - 1].split(',').collect::<Vec<_>>();

        let fill = if parts.len() == 2 {
            Fill::One {
                x: parts[0].parse().ok()?,
                y: parts[1].parse().ok()?,
            }
        } else if parts.len() == 4 {
            Fill::Rect {
                x1: parts[0].parse().ok()?,
                y1: parts[1].parse().ok()?,
                x2: parts[2].parse().ok()?,
                y2: parts[3].parse().ok()?,
            }
        } else {
            return None;
        };

        Some(Pixel {
            timestamp,
            user_id,
            color,
            fill,
        })
    }) {
        res.push(result.ok_or(Error::Deserialize)?);
    }
    println!("Finished {}.", num);

    Ok(res)
}
