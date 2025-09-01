use std::env;
use std::fs::File;
use std::io::{self, Write, BufReader, Read};
use std::path::PathBuf;
use std::time::Instant;
use encoding_rs::{Encoding, UTF_8, GBK};

const DEFAULT_CHUNK_SIZE: usize = 100 * 1024 * 1024; // 100MB default
const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB read buffer
const DEFAULT_LINE_ENDING: &str = "\n"; // 默认换行符

#[derive(Debug)]
struct Config {
    input_path: String,
    output_prefix: String,
    chunk_size: usize,
    line_ending: String,
    encoding: &'static Encoding,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();
        
        if args.len() < 3 {
            return Err(format!(
                "用法: {} <input_file> <output_prefix> [chunk_size_mb] [line_ending] [encoding]
                选项:
                chunk_size_mb: 分块大小(MB)
                line_ending:
                  LF     - Unix 风格 (\\n)
                  CRLF   - Windows 风格 (\\r\\n)
                  CR     - 经典 Mac 风格 (\\r)
                  custom - 自定义换行符(例如: custom:\\r\\n\\r\\n)
                encoding:
                  UTF-8  - UTF-8 编码
                  GBK    - GBK 编码", 
                args[0]
            ));
        }

        let input_path = args[1].clone();
        let output_prefix = args[2].clone();
        
        let chunk_size = if args.len() >= 4 {
            args[3].parse::<usize>()
                .map_err(|_| "无效的块大小")?
                * 1024 * 1024
        } else {
            DEFAULT_CHUNK_SIZE
        };

        let line_ending = if args.len() >= 5 {
            match args[4].to_uppercase().as_str() {
                "LF" => String::from("\n"),
                "CRLF" => String::from("\r\n"),
                "CR" => String::from("\r"),
                custom if custom.starts_with("CUSTOM:") => {
                    let custom_ending = custom[7..].to_string()
                        .replace("\\n", "\n")
                        .replace("\\r", "\r");
                    if custom_ending.is_empty() {
                        return Err("自定义换行符不能为空".to_string());
                    }
                    custom_ending
                }
                _ => return Err("无效的换行符选项. 请使用 LF, CRLF, CR 或 custom:xxx".to_string())
            }
        } else {
            String::from(DEFAULT_LINE_ENDING)
        };

        let encoding = if args.len() >= 6 {
            match args[5].to_uppercase().as_str() {
                "UTF-8" => UTF_8,
                "GBK" => GBK,
                _ => return Err("不支持的编码. 目前支持: UTF-8, GBK".to_string())
            }
        } else {
            UTF_8
        };

        Ok(Config {
            input_path,
            output_prefix,
            chunk_size,
            line_ending,
            encoding,
        })
    }
}

fn find_last_line_ending(data: &[u8], line_ending: &str, encoding: &'static Encoding) -> Option<usize> {
    if data.is_empty() {
        return None;
    }

    // 解码数据
    let (decoded, _, had_errors) = encoding.decode(data);
    if had_errors {
        eprintln!("警告: 发现无效的字符编码");
    }

    // 在解码后的文本中查找换行符
    if let Some(last_pos) = decoded.rfind(line_ending) {
        // 将字符位置转换回字节位置
        let byte_pos = encoding
            .encode(&decoded[..last_pos])
            .0
            .len();
        Some(byte_pos)
    } else {
        None
    }
}

fn write_compressed_chunk(chunk: &[u8], output_prefix: &str, chunk_number: usize) -> io::Result<()> {
    // 创建输出文件路径
    let output_path = PathBuf::from(format!("{}.{:03}.zst", output_prefix, chunk_number));
    
    // 压缩数据
    let compressed = zstd::encode_all(chunk, 3)?;
    
    // 写入文件
    let mut output_file = File::create(output_path.clone())?;
    output_file.write_all(&compressed)?;
    
    println!("写入分卷 {} (压缩后 {} 字节)", chunk_number, compressed.len());
    Ok(())
}

fn main() -> io::Result<()> {
    let start_time = Instant::now();
    
    // 解析配置
    let config = match Config::from_args() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("错误: {}", e);
            return Ok(());
        }
    };

    println!("使用配置:");
    println!("- 编码: {}", config.encoding.name());
    println!("- 换行符: {}", config.line_ending.escape_default());
    println!("- 分块大小: {} MB", config.chunk_size / 1024 / 1024);

    // 初始化文件读取
    let file = File::open(&config.input_path)?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut current_chunk = Vec::with_capacity(config.chunk_size + BUFFER_SIZE);
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);
    let mut chunk_number = 1;
    let mut last_newline_pos = 0;
    let mut total_bytes = 0;
    
    loop {
        buffer.clear();
        let n = reader.by_ref().take(BUFFER_SIZE as u64).read_to_end(&mut buffer)?;
        if n == 0 && current_chunk.is_empty() {
            break;
        }

        if n > 0 {
            // 查找最后一个换行符的位置
            let mut end_pos = if n == 0 { buffer.len() } else { n };
            if !buffer.is_empty() {
                if let Some(last_pos) = find_last_line_ending(&buffer[..end_pos], &config.line_ending, config.encoding) {
                    end_pos = last_pos + config.line_ending.len();
                }
            }

            // 将数据添加到当前块
            current_chunk.extend_from_slice(&buffer[..end_pos]);
            total_bytes += end_pos;

            // 如果当前块超过目标大小，在最后一个换行符处分割
            if current_chunk.len() >= config.chunk_size {
                if let Some(last_pos) = find_last_line_ending(&current_chunk[last_newline_pos..], &config.line_ending, config.encoding) {
                    let split_pos = last_newline_pos + last_pos + config.line_ending.len();
                    
                    // 写入到分割位置的数据
                    write_compressed_chunk(&current_chunk[..split_pos], &config.output_prefix, chunk_number)?;
                    
                    // 保留剩余数据
                    let remaining = current_chunk[split_pos..].to_vec();
                    current_chunk.clear();
                    current_chunk.extend(remaining);
                    last_newline_pos = 0;
                    chunk_number += 1;
                }
            }

            // 如果还有剩余数据，移动到下一个缓冲区
            if end_pos < buffer.len() {
                current_chunk.extend_from_slice(&buffer[end_pos..]);
            }
        }

        // 处理最后的数据块
        if n == 0 && !current_chunk.is_empty() {
            write_compressed_chunk(&current_chunk, &config.output_prefix, chunk_number)?;
            break;
        }
    }

    let duration = start_time.elapsed();
    println!("\n压缩统计:");
    println!("- 总分卷数: {}", chunk_number);
    println!("- 总数据量: {:.2} MB", total_bytes as f64 / 1024.0 / 1024.0);
    println!("- 处理耗时: {:.2} 秒", duration.as_secs_f64());
    println!("- 平均速度: {:.2} MB/s", (total_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64());
    
    Ok(())
}
