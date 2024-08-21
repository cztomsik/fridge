pub const Error = error{
    OutOfMemory,
    CheckViolation,
    ForeignKeyViolation,
    NotNullViolation,
    UniqueViolation,
    DbError,
};
