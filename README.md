# r3k

Collect historical Russell 3000 constitutents and GICS industries from iShares N-CSR filings.

# HOWTO

Linux only.

```bash
# install 
git clone https://github.com/mjudell/r3k.git
pip install ./r3k

# set up directories
mkdir -p ncsr/raw
mkdir -p ncsr/parsed

# pull historical filings
r3k pull \
    --user-agent "Your Name name@domain.com"\
    --output ncsr/raw

# parse historical filings
r3k parse \
    --input ncsr/raw \
    --output ncsr/parsed
```
