(module
  (type (;0;) (func (param i64) (result i64)))
  (func (;0;) (type 0) (param i64) (result i64)
    (local i64 i32)
    local.get 0
    i64.const 2
    i64.lt_s
    if  ;; label = @1
      local.get 0
      return
    end
    loop  ;; label = @1
      local.get 0
      i64.const 1
      i64.sub
      call 0
      local.get 1
      i64.add
      local.set 1
      local.get 0
      i64.const 3
      i64.gt_s
      local.set 2
      local.get 0
      i64.const 2
      i64.sub
      local.set 0
      local.get 2
      br_if 0 (;@1;)
    end
    local.get 0
    local.get 1
    i64.add)
  (func (;1;) (type 0) (param i64) (result i64)
    (local i32 i64)
    memory.size
    local.set 1
    i32.const 1
    memory.grow
    drop
    i64.const 3026418949592973312
    local.set 2
    local.get 1
    i32.const 16
    i32.shl
    local.tee 1
    local.get 0
    i64.const -4294967297
    i64.le_u
    if (result i64)  ;; label = @1
      local.get 0
      i32.wrap_i64
      i64.load
      local.tee 0
      i64.const 56
      i64.shl
      local.get 0
      i64.const 40
      i64.shl
      i64.const 71776119061217280
      i64.and
      i64.or
      local.get 0
      i64.const 24
      i64.shl
      i64.const 280375465082880
      i64.and
      local.get 0
      i64.const 8
      i64.shl
      i64.const 1095216660480
      i64.and
      i64.or
      i64.or
      local.get 0
      i64.const 8
      i64.shr_u
      i64.const 4278190080
      i64.and
      local.get 0
      i64.const 24
      i64.shr_u
      i64.const 16711680
      i64.and
      i64.or
      local.get 0
      i64.const 40
      i64.shr_u
      i64.const 65280
      i64.and
      local.get 0
      i64.const 56
      i64.shr_u
      i64.or
      i64.or
      i64.or
      call 0
      local.tee 0
      i64.const 56
      i64.shl
      local.get 0
      i64.const 40
      i64.shl
      i64.const 71776119061217280
      i64.and
      i64.or
      local.get 0
      i64.const 24
      i64.shl
      i64.const 280375465082880
      i64.and
      local.get 0
      i64.const 8
      i64.shl
      i64.const 1095216660480
      i64.and
      i64.or
      i64.or
      local.get 0
      i64.const 8
      i64.shr_u
      i64.const 4278190080
      i64.and
      local.get 0
      i64.const 24
      i64.shr_u
      i64.const 16711680
      i64.and
      i64.or
      local.get 0
      i64.const 40
      i64.shr_u
      i64.const 65280
      i64.and
      local.get 0
      i64.const 56
      i64.shr_u
      i64.or
      i64.or
      i64.or
    else
      local.get 2
    end
    i64.store
    local.get 1
    i64.extend_i32_u
    i64.const 34359738368
    i64.or)
  (memory (;0;) 2)
  (global (;0;) i32 (i32.const 1024))
  (export "memory" (memory 0))
  (export "fib" (func 1))
  (export "_scylla_abi" (global 0))
  (data (;0;) (i32.const 1024) "\01"))
