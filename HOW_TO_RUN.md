# How to run the Listener

The Listener is a small program that calls Polymarket, stays on the line, and prints every change on the order boards of Polymarket's 3 busiest questions. No account or password needed.

## Step 1: Install Python (one time)

1. Go to https://www.python.org/downloads/ and click the big yellow **Download Python** button.
2. Open the file you downloaded and click through the installer.
   - **Windows:** on the first screen, tick the box **"Add python.exe to PATH"** before clicking Install.

## Step 2: Open the Terminal

The Terminal is a window where you type commands instead of clicking.

- **Mac:** press Cmd + Space, type `Terminal`, press Enter.
- **Windows:** press the Windows key, type `cmd`, press Enter.

## Step 3: Go to the project folder

Type `cd ` (with a space after it), then drag the project folder into the Terminal window and press Enter. That tells the Terminal "work inside this folder."

## Step 4: Install the 3 helper tools (one time)

Copy this line, paste it into the Terminal, press Enter:

- **Mac:** `python3 -m pip install -r requirements.txt`
- **Windows:** `python -m pip install -r requirements.txt`

Wait until it finishes and you can type again.

## Step 5: Run it

- **Mac:** `python3 listener.py`
- **Windows:** `python listener.py`

### Watch one specific event instead

Every Polymarket event has a link like `polymarket.com/event/laliga-2027-champion-20260701200737375`. The part after `/event/` is its name tag (the "slug"). Put it after `listener.py`:

- **Mac:** `python3 listener.py laliga-2027-champion-20260701200737375`
- **Windows:** `python listener.py laliga-2027-champion-20260701200737375`

The Listener then watches every question in that event (for LaLiga, one per team).

## What you will see

```
Asking Polymarket for its busiest questions...
  - Will Luiz Inácio Lula da Silva win the 2026 Brazilian presidential election?
Connected. Listening... (press Ctrl+C to stop)

FULL BOARD  YES | Will Luiz Inácio Lula da Silva...: best buy 0.703, best sell 0.717
UPDATE      YES | Will Luiz Inácio Lula da Silva...: buyers at 0.281 now total 20
TRADE       YES | Will Luiz Inácio Lula da Silva...: 3.53 traded at 0.708
```

- **FULL BOARD:** the whole board at the start. "Best buy" is the most anyone will pay; "best sell" is the least anyone will sell for.
- **UPDATE:** one price on the board changed. "Now total 20" means 20 shares are now waiting at that price (0 means nobody is left there).
- **TRADE:** someone actually bought and sold.
- **YES / NO:** every question has two sides, each with its own board. They mirror each other: buying YES at 0.30 is like selling NO at 0.70, so most updates show up on both.

## Step 6: Stop it

Press **Ctrl + C** (hold Control, press C). It will say "Stopped. Bye!"

## If something goes wrong

- **"python3: command not found"** (Mac) or **"'python' is not recognized"** (Windows): Python is not installed, or on Windows the "Add to PATH" box was not ticked. Reinstall Python and tick it.
- **"No module named ..."**: Step 4 did not finish. Run it again.
- **"Line dropped ... Calling back"**: your internet blinked. The Listener reconnects by itself.
