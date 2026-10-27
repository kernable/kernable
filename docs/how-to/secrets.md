# Passwords and other secrets

A command can carry a value that must never be stored: a password, a one-time code. Mark the field `secret`:

<<< @/examples/shop/ts/domain.ts#vault{7-8}

- The client moves the field to the vault before signing the command; the stored command keeps a reference.
- The worker gets the value in clear for as long as the command runs.
- The vault deletes it as soon as the command completed, committed or rejected; a kernel that restarts deletes those of commands completed meanwhile.

## Rules for handlers

- **Never put a secret into an event.** Store a hash, salted and slow (scrypt, Argon2), and mark it personal so it is erased with the person.
- **Derive the salt with `ctx.id()`.** Handlers must decide the same way when a command runs again; a random salt would differ.
- **Compare in a command**, like `SignIn`: the stored hash stays in the aggregate, and the answer is the outcome (committed or `denied`).
- **One-time codes:** store a hash with an expiry in an event, and send the code itself as a secret when it is used.

A secret is part of the command's signature through its reference: nobody can swap the value without breaking the signature.
