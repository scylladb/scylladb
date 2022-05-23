(module
  (type (;0;) (func (param i64) (result i64)))
  (func (;0;) (type 0) (param i64) (result i64)
    (local i32 i32 i64)
    i64.const -4294967296
    local.set 3
    local.get 0
    i64.const -4294967296
    i64.and
    i64.const 68719476736
    i64.eq
    if (result i64)  ;; label = @1
      local.get 0
      i32.wrap_i64
      local.tee 1
      i32.const 4
      i32.add
      local.tee 2
      local.get 1
      i32.const 12
      i32.add
      i32.load
      local.tee 1
      i32.const 24
      i32.shl
      local.get 1
      i32.const 8
      i32.shl
      i32.const 16711680
      i32.and
      i32.or
      local.get 1
      i32.const 8
      i32.shr_u
      i32.const 65280
      i32.and
      local.get 1
      i32.const 24
      i32.shr_u
      i32.or
      i32.or
      f32.convert_i32_s
      local.get 2
      i32.load
      local.tee 1
      i32.const 24
      i32.shl
      local.get 1
      i32.const 8
      i32.shl
      i32.const 16711680
      i32.and
      i32.or
      local.get 1
      i32.const 8
      i32.shr_u
      i32.const 65280
      i32.and
      local.get 1
      i32.const 24
      i32.shr_u
      i32.or
      i32.or
      f32.convert_i32_s
      f32.div
      i32.reinterpret_f32
      local.tee 1
      i32.const 24
      i32.shl
      local.get 1
      i32.const 8
      i32.shl
      i32.const 16711680
      i32.and
      i32.or
      local.get 1
      i32.const 8
      i32.shr_u
      i32.const 65280
      i32.and
      local.get 1
      i32.const 24
      i32.shr_u
      i32.or
      i32.or
      i32.store
      local.get 2
      i64.extend_i32_s
      i64.const 17179869184
      i64.or
    else
      local.get 3
    end)
  (memory (;0;) 2)
  (global (;0;) i32 (i32.const 1024))
  (export "memory" (memory 0))
  (export "div" (func 0))
  (export "_scylla_abi" (global 0))
  (data (;0;) (i32.const 1024) "\01"))
