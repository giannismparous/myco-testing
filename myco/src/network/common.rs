//! # Myco Network Common Module
//!
//! This module contains shared types and traits for network communication in the Myco library.

use crate::error::MycoError;
use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, MycoError>;
